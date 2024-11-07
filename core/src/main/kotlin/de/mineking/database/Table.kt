package de.mineking.database

import org.jdbi.v3.core.kotlin.useHandleUnchecked
import org.jdbi.v3.core.result.ResultIterable
import java.lang.reflect.InvocationHandler
import java.lang.reflect.InvocationTargetException
import java.lang.reflect.Method
import kotlin.reflect.*
import kotlin.reflect.jvm.javaField
import kotlin.reflect.jvm.kotlinFunction

interface Table<T: Any> {
	val structure: TableStructure<T>
	val implementation: TableImplementation<T>

	fun identifyObject(obj: T) = identifyObject(structure, obj)

	fun createTable()
	fun dropTable() = structure.manager.driver.useHandleUnchecked { it.createUpdate("drop table ${ structure.name }").execute() }

	fun selectRowCount(where: Where = Where.EMPTY): Int
	fun <C> selectValue(target: Node<C>, type: KType, where: Where = Where.EMPTY, order: Order? = null, limit: Int? = null, offset: Int? = null): QueryResult<C>

	fun select(vararg columns: Node<*>, where: Where = Where.EMPTY, order: Order? = null, limit: Int? = null, offset: Int? = null): QueryResult<T>
	fun select(vararg columns: KProperty<*>, where: Where = Where.EMPTY, order: Order? = null, limit: Int? = null, offset: Int? = null): QueryResult<T> = select(columns = columns.map { property(it) }.toTypedArray(), where, order, limit, offset)
	fun select(where: Where = Where.EMPTY, order: Order? = null, limit: Int? = null, offset: Int? = null): QueryResult<T> = select(columns = emptyArray<KProperty<*>>(), where, order, limit, offset)

	fun update(obj: T): UpdateResult<T>
	fun update(vararg columns: Pair<Node<*>, Node<*>>, where: Where = Where.EMPTY): UpdateResult<Int>

	fun insert(obj: T): UpdateResult<T>
	fun upsert(obj: T): UpdateResult<T>

	fun delete(where: Where = Where.EMPTY): Int
	fun delete(obj: T) = delete(identifyObject(obj))
}

inline fun <reified T> Table<*>.selectValue(target: Node<T>, where: Where = Where.EMPTY, order: Order? = null, limit: Int? = null, offset: Int? = null): QueryResult<T> = selectValue(target, typeOf<T>(), where, order, limit, offset)

abstract class TableImplementation<T: Any>(
	val type: KClass<*>,
	override val structure: TableStructure<T>,
	val instance: () -> T
) : Table<T>, InvocationHandler {
	override val implementation: TableImplementation<T> = this

	open fun parseResult(context: ReadContext) {
		@Suppress("UNCHECKED_CAST")
		val instance = context.instance as T

		fun <C> setField(column: DirectColumnData<T, C>) = column.set(instance, column.mapper.read(column, column.type, context, column.name))
		structure.columns.forEach { if (context.shouldRead(it.name)) setField(it) }

		if (context.instance is DataObject<*>) context.instance.afterRead()
	}

	private fun createJoinList(columns: Collection<DirectColumnData<*, *>>, prefix: Array<String> = emptyArray()): List<String> {
		val temp = columns.filter { it.reference != null }.filter { !it.type.isArray() }

		if (temp.isEmpty()) return emptyList()
		return temp.flatMap { listOf("""
			left join ${ it.reference!!.structure.name } 
			as "${ (prefix + it.name).joinToString(".") }" 
			on ${ (
				unsafeNode("\"${ (prefix + it.name).joinToString(".") }\".\"${ it.reference!!.structure.getKeys().first().name }\"")
						isEqualTo
						unsafeNode("\"${ prefix.joinToString(".").takeIf { it.isNotBlank() } ?: structure.name }\".\"${ it.name }\"")
				).get(structure) }
		""") + createJoinList(it.reference!!.structure.columns, prefix + it.name) }
	}

	override fun selectRowCount(where: Where): Int {
		val sql = """
			select count(*) from ${ structure.name }
			${ createJoinList(structure.columns).joinToString(" ") }
			${ where.format(structure) }
		""".trim().replace("\\s+".toRegex(), " ")

		return structure.manager.execute { it.createQuery(sql)
			.bindMap(where.values(structure))
			.mapTo(Int::class.java)
			.first()
		}
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

	override fun select(vararg columns: Node<*>, where: Where, order: Order?, limit: Int?, offset: Int?): QueryResult<T> {
		fun convert(columns: Collection<ColumnData<*, *>>): List<Node<Any>> = columns.flatMap { column -> when (column) {
			is DirectColumnData -> convert(column.getChildren().filter { it.shouldCreate() }) + property(column.property.name)
			is VirtualColumnData -> listOf(property("${ column.parent.property.name }.${ column.simpleName }"))
			else -> error("")
		} }
		fun createColumnList(columns: Collection<Node<*>>, context: TableStructure<*>, prefix: Array<String> = emptyArray()): List<Pair<String, Pair<String, String>>> {
			if (columns.isEmpty()) return emptyList()

			return columns.flatMap {
				val column = it.columnContext(context)!!.column

				listOf(it.format(context) {
					val temp = it.context.toMutableList()
					if (temp.isNotEmpty()) temp.removeAt(0)
					temp.addAll(0, prefix.toList())

					it.copy(context = temp.toTypedArray()).build()
				} to ((prefix.joinToString(".").takeIf { it.isNotBlank() } ?: structure.name) to column.name)) +
						if (column is DirectColumnData && column.reference != null && !column.type.isArray()) createColumnList(convert(column.reference!!.structure.getAllColumns()), column.reference!!.structure, prefix + column.name)
						else emptyList()
			}
		}

		val columnList = createColumnList(
			if (columns.isEmpty()) convert(structure.getAllColumns())
			else columns.toSet(),
			structure
		)

		val sql = createSelect(columnList.joinToString { (value, name) -> "$value as \"${ name.first }.${ name.second }\"" }, where, order, limit, offset)
		return object : SimpleQueryResult<T> {
			override fun <O> execute(handler: (ResultIterable<T>) -> O): O = structure.manager.execute { handler(it.createQuery(sql)
				.bindMap(columns.flatMap { it.values(structure, it.columnContext(structure)?.column).entries }.associate { it.toPair() })
				.bindMap(where.values(structure))
				.map { set, _ ->
					val instance = instance()
					parseResult(ReadContext(instance, structure, set, columnList.map { (_, name) -> "${ name.first }.${ name.second }" }))
					instance
				}
			) }
		}
	}

	override fun <C> selectValue(target: Node<C>, type: KType, where: Where, order: Order?, limit: Int?, offset: Int?): QueryResult<C> {
		val column = target.columnContext(structure)
		val mapper = structure.manager.getTypeMapper<C, Any>(type, column?.column?.getRootColumn()?.property) ?: throw IllegalArgumentException("No suitable TypeMapper found for $type")

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
		return object : SimpleQueryResult<C> {
			override fun <O> execute(handler: (ResultIterable<C>) -> O): O = structure.manager.execute { handler(it.createQuery(sql)
				.bindMap(target.values(structure, column?.column))
				.bindMap(where.values(structure))
				.map { set, _ -> mapper.read(column?.column?.getRootColumn(), type, ReadContext(it, structure, set, columnList.map { "${ it.first }.${ it.second }" } + "value", autofillPrefix = { it != "value" }, shouldRead = false), "value") }
			) }
		}
	}

	private fun executeUpdate(update: org.jdbi.v3.core.statement.Update, obj: T) {
		val columns = structure.getAllColumns()

		columns.forEach {
			fun <C> createArgument(column: ColumnData<T, C>) = column.mapper.write(column, structure, column.type, column.get(obj))
			update.bind(it.name, createArgument(it))
		}

		update.execute { stmt, ctx -> ctx.use {
			val statement = stmt.get()
			val set = statement.resultSet

			if (set.next()) parseResult(ReadContext(obj, structure, set, columns.filter { it.getRootColumn().reference == null }.map { it.name }, autofillPrefix = { false }))
		} }
	}

	abstract fun <T> createResult(function: () -> T): UpdateResult<T>

	override fun update(obj: T): UpdateResult<T> {
		if (obj is DataObject<*>) obj.beforeWrite()
		val identity = identifyObject(obj)

		val columns = structure.getAllColumns().filter { !it.getRootColumn().key }

		val sql = """
			update ${ structure.name }
			set ${columns.joinToString { "\"${ it.name }\" = :${ it.name }" }}
			${ identity.format(structure) }
			returning *
		""".trim().replace("\\s+".toRegex(), " ")

		return createResult {
			structure.manager.execute { executeUpdate(it.createUpdate(sql).bindMap(identity.values(structure)), obj) }
			if (obj is DataObject<*>) obj.afterRead()
			obj
		}
	}

	override fun update(vararg columns: Pair<Node<*>, Node<*>>, where: Where): UpdateResult<Int > {
		val specs = columns.associate { (column, value) -> (column to column.columnContext(structure)!!) to (value to value.columnContext(structure)) }

		require(specs.all { (column) -> column.second.context.isEmpty() }) { "Cannot update reference, update in the table directly" }
		require(specs.none { (column) -> column.second.column.getRootColumn().key }) { "Cannot update key" }

		val sql = """
			update ${ structure.name } 
			set ${ specs.entries.joinToString { (column, value) -> "${ column.first.format(structure) { it.build(prefix = false) } } = ${ value.first.format(structure) }" } }
			${ where.format(structure) } 
		""".trim().replace("\\s+".toRegex(), " ")

		return createResult { structure.manager.execute { it.createUpdate(sql)
			.bindMap(specs.flatMap { (column, value) -> column.first.values(structure, column.second.column).entries + value.first.values(structure, value.second?.column ?: column.second.column).entries }.associate { it.toPair() })
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
			(${ columns.joinToString { "\"${ it.name }\"" } })
			values(${ columns.joinToString { ":${ it.name }" } }) 
			returning *
		""".trim().replace("\\s+".toRegex(), " ")

		return createResult {
			structure.manager.execute { executeUpdate(it.createUpdate(sql), obj) }
			if (obj is DataObject<*>) obj.afterRead()
			obj
		}
	}

	override fun upsert(obj: T): UpdateResult<T> {
		if (obj is DataObject<*>) obj.beforeWrite()

		val insertColumns = structure.getAllColumns().filter {
			if (!it.getRootColumn().autogenerate) true
			else {
				val value = it.get(obj)
				value != 0 && value != null
			}
		}

		val updateColumns = structure.getAllColumns().filter { !it.getRootColumn().key }


		val sql = """
			insert into ${ structure.name }
			(${ insertColumns.joinToString { "\"${ it.name }\"" } })
			values(${ insertColumns.joinToString { ":${ it.name }" } }) 
			on conflict (${ structure.getKeys().joinToString { "\"${ it.name }\"" } }) do update set
			${ updateColumns.joinToString { "\"${ it.name }\" = :${ it.name }" } }
			returning *
		""".trim().replace("\\s+".toRegex(), " ")

		return createResult {
			structure.manager.execute { executeUpdate(it.createUpdate(sql), obj) }
			if (obj is DataObject<*>) obj.afterRead()
			obj
		}
	}

	/**
	 * Does not support reference conditions (because postgres doesn't allow join in delete)
	 */
	override fun delete(where: Where): Int {
		val sql = "delete from ${ structure.name } ${ where.format(structure) }"
		return structure.manager.execute { it.createUpdate(sql)
			.bindMap(where.values(structure))
			.execute()
		}
	}

	override fun invoke(proxy: Any?, method: Method?, args: Array<out Any?>?): Any? {
		require(method != null)

		fun createCondition() = allOf(if (args == null) emptyList() else method.parameters
			.mapIndexed { index, value -> index to value }
			.filter { (_, it) -> it.isAnnotationPresent(Condition::class.java) }
			.map { (index, param) -> property<Any>(param.getAnnotation(Condition::class.java)!!.name.takeIf { it.isNotBlank() } ?: param.name) + " ${ param.getAnnotation(Condition::class.java)!!.operation } " + value(args[index]) }
			.map { Where(it) }
		)

		when {
			method.isAnnotationPresent(Select::class.java) -> {
				val result = select(where = createCondition())

				return when {
					method.returnType == QueryResult::class.java -> result
					method.returnType == List::class.java -> result.list()
					method.kotlinFunction?.returnType?.isMarkedNullable == true -> result.findFirst()
					else -> result.first()
				}
			}

			method.isAnnotationPresent(Insert::class.java) || method.isAnnotationPresent(Upsert::class.java) -> {
				require(args != null)

				val obj = instance()

				method.parameters
					.mapIndexed { index, value -> index to value }
					.filter { (_, it) -> it.isAnnotationPresent(Parameter::class.java) }
					.forEach { (index, param) ->
						val name = param.getAnnotation(Parameter::class.java)!!.name.takeIf { it.isNotBlank() } ?: param.name

						@Suppress("UNCHECKED_CAST")
						val column = structure.getColumnFromCode(name) as DirectColumnData<T, Any>? ?: error("Column $name not found")
						val value = args[index]

						column.set(obj, value)
					}

				val result = if (method.isAnnotationPresent(Insert::class.java)) insert(obj) else upsert(obj)
				return when {
					method.returnType == UpdateResult::class.java -> result
					else -> result.getOrThrow()
				}
			}

			method.isAnnotationPresent(Update::class.java) -> {
				require(args != null)

				val updates = method.parameters
					.mapIndexed { index, value -> index to value }
					.filter { (_, it) -> it.isAnnotationPresent(Parameter::class.java) }
					.map { (index, param) -> property<Any>(param.getAnnotation(Parameter::class.java)!!.name.takeIf { it.isNotBlank() } ?: param.name) to value(args[index]) }

				val result = update(columns = updates.toTypedArray(), where = createCondition())
				return when {
					method.returnType == UpdateResult::class.java -> result
					else -> result.getOrThrow()
				}
			}

			method.isAnnotationPresent(Delete::class.java) -> return delete(where = createCondition())
		}

		return try {
			javaClass.getMethod(method.name, *method.parameterTypes).invoke(this, *(args ?: emptyArray()))
		} catch(_: NoSuchMethodException) {
			type.java.classes.find { it.simpleName == "DefaultImpls" }?.getMethod(method.name, type.java, *method.parameterTypes)?.invoke(null, proxy, *(args ?: emptyArray()))
		} catch(e: IllegalAccessException) {
			throw RuntimeException(e)
		} catch(e: InvocationTargetException) {
			throw e.cause!!
		}
	}
}

sealed class ColumnData<O: Any, C>(
	val table: TableStructure<O>,
	val baseName: String,
	val name: String,
	val mapper: TypeMapper<C, *>,
	val type: KType
) {
	abstract fun get(obj: O): C
	abstract fun getRootColumn(): DirectColumnData<O, *>

	open fun shouldCreate(): Boolean = true
}

abstract class VirtualColumnData<O: Any, C> internal constructor(
	table: TableStructure<O>,
	baseName: String,
	name: String,
	val simpleName: String,
	mapper: TypeMapper<C, *>,
	type: KType,
	val parent: DirectColumnData<O, *>,
	val transform: ((String) -> String)?
) : ColumnData<O, C>(table, baseName, name, mapper, type) {
	override fun getRootColumn(): DirectColumnData<O, *> = parent
}

class DirectColumnData<O: Any, C> internal constructor(
	table: TableStructure<O>,
	baseName: String,
	name: String,
	mapper: TypeMapper<C, *>,
	val property: KProperty1<O, C>,
	val key: Boolean,
	val autogenerate: Boolean
) : ColumnData<O, C>(table, baseName, name, mapper, property.returnType) {
	var reference: Table<*>? = null
	private val children: MutableList<VirtualColumnData<O, *>> = arrayListOf()

	override fun get(obj: O): C = property.get(obj)
	fun set(obj: O, value: C?) = property.javaField?.apply { trySetAccessible() }?.set(obj, value) //Allow writing final

	fun getChildren(): List<VirtualColumnData<O, *>> = children

	override fun getRootColumn(): DirectColumnData<O, *> = this
	fun <T> createVirtualChild(baseName: String, name: String, simpleName: String, mapper: TypeMapper<T, *>, type: KType, isReference: Boolean = false, transform: ((String) -> String)? = null, value: (O) -> T): VirtualColumnData<O, T> {
		val column = object : VirtualColumnData<O, T>(table, baseName, name, simpleName, mapper, type, this, transform) {
			override fun get(obj: O): T = value(obj)
			override fun shouldCreate(): Boolean = !isReference
		}

		children.add(column)
		return column
	}
}

data class TableStructure<T: Any>(
	val manager: DatabaseConnection,
	val name: String,
	val namingStrategy: NamingStrategy,
	val columns: List<DirectColumnData<T, *>>
) {
	fun getColumnFromDatabase(name: String): ColumnData<T, *>? = getAllColumns().find { it.name == name }
	fun getColumnFromCode(name: String): DirectColumnData<T, *>? = columns.find { it.property.name == name }

	fun getAllColumns(): List<ColumnData<T, *>> {
		val result = arrayListOf<ColumnData<T, *>>()

		result += columns
		result += columns.flatMap { it.getChildren() }.filter { result.none { test -> test.name == it.name } }

		return result.filter { it.shouldCreate() }
	}

	fun getKeys(): List<ColumnData<T, *>> = getAllColumns().filter { it.getRootColumn().key }.filter { it.shouldCreate() }
}