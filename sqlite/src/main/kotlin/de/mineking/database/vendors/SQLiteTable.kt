package de.mineking.database.vendors

import de.mineking.database.*
import de.mineking.database.vendors.SQLiteConnection.Companion.logger
import org.jdbi.v3.core.kotlin.useHandleUnchecked
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.jdbi.v3.core.result.ResultIterable
import org.jdbi.v3.core.statement.UnableToExecuteStatementException
import org.jdbi.v3.core.statement.Update
import org.sqlite.SQLiteErrorCode
import org.sqlite.SQLiteException
import kotlin.reflect.KClass
import kotlin.reflect.KType

class SQLiteTable<T: Any>(
	type: KClass<*>,
	structure: TableStructure<T>,
	instance: () -> T
) : TableImplementation<T>(type, structure, instance) {
	override fun createTable() {
		val columns = structure.getAllColumns().filter { it.shouldCreate() }
		require(columns.filter { it.getRootColumn().property.hasDatabaseAnnotation<Unique>() }.groupBy { it.getRootColumn().property.getDatabaseAnnotation<Unique>()!!.name }.none { it.value.size > 1 }) { "Complex unique constraint not supported in SQLite" }

		fun <C> formatColumn(column: ColumnData<T, C>): String {
			val root = column.getRootColumn()
			return """
				"${ column.name }" ${ column.mapper.getType(column, column.table, root.property, column.type).sqlName }
				${ if (column.getRootColumn().getRootColumn().property.hasDatabaseAnnotation<Unique>()) " unique" else "" }
				${ root.property.takeIf { !it.returnType.isMarkedNullable }?.let { " not null" } ?: "" }
				${ if (root.key) " primary key" else "" }
				${ if (root.autogenerate) root.property.getDatabaseAnnotation<AutoGenerate>()?.generator?.takeIf { it.isNotBlank() }?.let { " default $it" } ?: " autoincrement" else "" }
			""".replace("\n", "").replace("\t", "")
		}

		structure.manager.driver.useHandleUnchecked { it.createUpdate("""
			create table if not exists ${ structure.name } (
				${ columns.map { formatColumn(it) }.joinToString() }
			)
		""".replace("\n", "").replace("\t", "")).execute() }

		structure.manager.driver.useHandleUnchecked { it.createQuery("select * from ${ structure.name } limit 1").execute { supplier, context ->
			val meta = supplier.get().resultSet.metaData
			if (columns.size != meta.columnCount) logger.warn("Number of columns in code and database do not match (Code: ${ columns.size }, Database: ${ meta.columnCount })")
			else for (i in 1 .. meta.columnCount) {
				val name = meta.getColumnName(i)
				val column = structure.getColumnFromDatabase(name)

				if (column == null) logger.warn("Column $name from database not found in code")
			}
		} }
	}


	override fun selectRowCount(where: Where): Int {
		val sql = """
			select count(*) from ${ structure.name }
			${ createJoinList(structure.columns).joinToString(" ") }
			${ where.format(structure) }
		""".trim().replace("\\s+".toRegex(), " ")

		return structure.manager.driver.withHandleUnchecked { it.createQuery(sql)
			.bindMap(where.values(structure))
			.mapTo(Int::class.java)
			.first()
		}
	}

	private fun createJoinList(columns: Collection<DirectColumnData<*, *>>, prefix: Array<String> = emptyArray()): List<String> {
		val temp = columns.filter { it.reference != null }.filter { !it.type.isArray() }

		if (temp.isEmpty()) return emptyList()
		return temp.flatMap { listOf("""
			left join ${ it.reference!!.structure.name } 
			as "${ (prefix + it.name).joinToString(".") }" 
			on ${ (
				unsafeNode("\"${(prefix + it.name).joinToString(".")}\".\"${it.reference!!.structure.getKeys().first().name}\"")
						isEqualTo
						unsafeNode("\"${prefix.joinToString(".").takeIf { it.isNotBlank() } ?: structure.name}\".\"${it.name}\"")
				).get(structure) }
		""") + createJoinList(it.reference!!.structure.columns, prefix + it.name) }
	}

	private fun createSelect(columns: String, where: Where, order: Order?, limit: Int?, offset: Int?): String = """
		select $columns
		from ${ structure.name }
		${ createJoinList(structure.columns.reversed()).joinToString(" ") }
		${ where.format(structure) } 
		${ order?.format() ?: "" } 
		${ limit?.let { "limit $it" } ?: "" }
		${ offset?.let { "offset $it" } ?: "" } 
	""".trim().replace("\\s+".toRegex(), " ")

	override fun select(vararg columns: String, where: Where, order: Order?, limit: Int?, offset: Int?): QueryResult<T> {
		fun createColumnList(columns: Collection<ColumnData<*, *>>, prefix: Array<String> = emptyArray()): List<Pair<String, String>> {
			if (columns.isEmpty()) return emptyList()
			return columns
				.filterIsInstance<DirectColumnData<*, *>>()
				.filter { it.reference != null }
				.filter { !it.type.isArray() }
				.flatMap { createColumnList(it.reference!!.structure.getAllColumns(), prefix + it.name) } +
					columns.map { (prefix.joinToString(".").takeIf { it.isNotBlank() } ?: structure.name) to it.name }
		}

		val columnList = createColumnList(
			if (columns.isEmpty()) structure.getAllColumns()
			else columns.map { parseColumnSpecification(it, structure).column }.toSet()
		)

		val sql = createSelect(columnList.joinToString { "\"${it.first}\".\"${it.second}\" as \"${it.first}.${it.second}\"" }, where, order, limit, offset)
		return object : RowQueryResult<T> {
			override val instance: () -> T = this@SQLiteTable.instance
			override fun <O> execute(handler: ((T) -> Boolean) -> O): O = structure.manager.driver.withHandleUnchecked { it.createQuery(sql)
				.bindMap(where.values(structure))
				.execute { stmt, _ ->
					val statement = stmt.get()
					val set = statement.resultSet

					handler { parseResult(ReadContext(it, structure, set, columnList.map { "${ it.first }.${ it.second }" })) }
				}
			}
		}
	}

	override fun <C> select(target: Node, type: KType, where: Where, order: Order?, limit: Int?, offset: Int?): QueryResult<C> {
		val column = target.columnContext(structure)
		val mapper = structure.manager.getTypeMapper<C, Any>(type, column?.column?.getRootColumn()?.property) ?: throw IllegalArgumentException("No suitable TypeMapper found")

		fun createColumnList(columns: List<ColumnData<*, *>>, prefix: Array<String> = emptyArray()): List<Pair<String, String>> {
			if (columns.isEmpty()) return emptyList()
			return columns
				.filterIsInstance<DirectColumnData<*, *>>()
				.filter { it.reference != null }
				.flatMap { createColumnList(it.reference!!.structure.getAllColumns(), prefix + it.name
				) } +
					(columns + columns.flatMap { if (it is DirectColumnData) it.getChildren() else emptyList() }).map { (prefix.joinToString(".").takeIf { it.isNotBlank() } ?: structure.name) to it.name }
		}

		val columnList = createColumnList(column?.column?.let { listOf(it) } ?: emptyList())

		val sql = createSelect((columnList.map { "\"${ it.first }\".\"${ it.second }\" as \"${ it.first }.${ it.second }\"" } + "(${ target.format(structure) }) as \"value\"").joinToString(), where, order, limit, offset)
		return object : ValueQueryResult<C> {
			override fun <O> execute(handler: (ResultIterable<C>) -> O): O = structure.manager.driver.withHandleUnchecked { handler(it.createQuery(sql)
				.bindMap(target.values(structure, column?.column))
				.bindMap(where.values(structure))
				.map { set, _ -> mapper.read(column?.column?.getRootColumn(), type, ReadContext(it, structure, set, columnList.map { "${ it.first }.${ it.second }" } + "value", autofillPrefix = { it != "value" }, shouldRead = false), "value") }
			) }
		}
	}

	private fun executeUpdate(update: Update, obj: T) {
		val columns = structure.getAllColumns()

		columns.forEach {
			fun <C> createArgument(column: ColumnData<T, C>) = column.mapper.write(column, structure, column.type, column.get(obj))
			update.bind(it.name, createArgument(it))
		}

		return update.execute { stmt, _ ->
			val statement = stmt.get()
			val set = statement.resultSet

			parseResult(ReadContext(obj, structure, set, columns.filter { it.getRootColumn().reference == null }.map { it.name }, autofillPrefix = { false }))
		}
	}

	private fun <T> createResult(function: () -> T): UpdateResult<T> {
		return try {
			UpdateResult(function(), null, uniqueViolation = false, notNullViolation = false)
		} catch (e: UnableToExecuteStatementException) {
			val sqlException = e.cause as SQLiteException
			val result = UpdateResult<T>(null, sqlException, sqlException.resultCode == SQLiteErrorCode.SQLITE_CONSTRAINT_UNIQUE || sqlException.resultCode == SQLiteErrorCode.SQLITE_CONSTRAINT_PRIMARYKEY, sqlException.resultCode == SQLiteErrorCode.SQLITE_CONSTRAINT_NOTNULL)

			if (!result.uniqueViolation && !result.notNullViolation) throw e
			result
		}
	}

	override fun update(obj: T): UpdateResult<T> {
		if (obj is DataObject<*>) obj.beforeWrite()
		val identity = identifyObject(obj)

		val columns = structure.getAllColumns().filter { !it.getRootColumn().key }

		val sql = """
			update ${ structure.name }
			set ${columns.joinToString { "\"${it.name}\" = :${it.name}" }}
			${ identity.format(structure) }
			returning *
		""".trim().replace("\\s+".toRegex(), " ")

		return createResult {
			structure.manager.driver.withHandleUnchecked { executeUpdate(it.createUpdate(sql).bindMap(identity.values(structure)), obj) }
			if (obj is DataObject<*>) obj.afterRead()
			obj
		}
	}

	override fun update(column: String, value: Node, where: Where): UpdateResult<Int > {
		val spec = parseColumnSpecification(column, structure)

		require(spec.context.isEmpty()) { "Cannot update reference, update in the table directly" }
		require(!spec.column.getRootColumn().key) { "Cannot update key" }

		val sql = """
			update ${ structure.name } 
			set ${ spec.build(false) } = ${ value.format(structure) }
			${ where.format(structure) } 
		""".trim().replace("\\s+".toRegex(), " ")

		return createResult { structure.manager.driver.withHandleUnchecked { it.createUpdate(sql)
			.bindMap(value.values(structure, spec.column))
			.bindMap(where.values(structure))
			.execute()
		} }
	}

	override fun insert(obj: T): UpdateResult<T> {
		if (obj is DataObject<*>) obj.beforeWrite()

		val columns = structure.getAllColumns().filter {
			if (!it.getRootColumn().autogenerate) true
			else {
				val value = it.get(obj)
				value != 0 && value != null
			}
		}

		val sql = """
			insert into ${ structure.name }
			(${columns.joinToString { "\"${it.name}\"" }})
			values(${columns.joinToString { ":${it.name}" }}) 
			returning *
		""".trim().replace("\\s+".toRegex(), " ")

		return createResult {
			structure.manager.driver.withHandleUnchecked { executeUpdate(it.createUpdate(sql), obj) }
			if (obj is DataObject<*>) obj.afterRead()
			obj
		}
	}

	override fun upsert(obj: T): UpdateResult<T> {
		TODO("Not yet implemented")
	}

	/**
	 * Does not support reference conditions (because postgres doesn't allow join in delete)
	 */
	override fun delete(where: Where): Int {
		val sql = "delete from ${ structure.name } ${ where.format(structure) }"
		return structure.manager.driver.withHandleUnchecked { it.createUpdate(sql)
			.bindMap(where.values(structure))
			.execute()
		}
	}
}