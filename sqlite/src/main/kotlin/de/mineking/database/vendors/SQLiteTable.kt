package de.mineking.database.vendors

import de.mineking.database.*
import de.mineking.database.vendors.SQLiteConnection.Companion.logger
import org.jdbi.v3.core.kotlin.useHandleUnchecked
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
		TODO("Not yet implemented")
	}

	override fun createSelect(columns: String, where: Where, order: Order?, limit: Int?, offset: Int?): String = """
		
	""".trim().replace("\\s+".toRegex(), " ")

	override fun select(vararg columns: String, where: Where, order: Order?, limit: Int?, offset: Int?): QueryResult<T> {
		TODO("Not yet implemented")
	}

	override fun <C> select(target: Node, type: KType, where: Where, order: Order?, limit: Int?, offset: Int?): QueryResult<C> {
		TODO("Not yet implemented")
	}

	override fun update(obj: T): UpdateResult<T> {
		TODO("Not yet implemented")
	}

	override fun update(column: String, value: Node, where: Where): UpdateResult<Int> {
		TODO("Not yet implemented")
	}

	override fun insert(obj: T): UpdateResult<T> {
		TODO("Not yet implemented")
	}

	override fun delete(where: Where): Int {
		TODO("Not yet implemented")
	}
}