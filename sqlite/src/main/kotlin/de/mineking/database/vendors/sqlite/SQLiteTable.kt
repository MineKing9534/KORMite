package de.mineking.database.vendors.sqlite

import de.mineking.database.*
import de.mineking.database.vendors.sqlite.SQLiteConnection.Companion.logger
import org.jdbi.v3.core.kotlin.useHandleUnchecked
import org.jdbi.v3.core.statement.UnableToExecuteStatementException
import org.sqlite.SQLiteErrorCode
import org.sqlite.SQLiteException
import kotlin.reflect.KClass

class SQLiteTable<T: Any>(
	tableType: KClass<*>,
	structure: TableStructure<T>,
	instance: () -> T
) : TableImplementation<T>(tableType, structure, instance) {
	override fun createTable() {
		val columns = structure.columns

		fun <C> formatColumn(column: ColumnData<T, C>): String {
			val type = column.mapper.getType(column, column.table, column.type)
			return """
				"${ column.name }" ${ type.sqlName }
				${ if (column.property.hasDatabaseAnnotation<AutoGenerate>()) " default ${ column.property.getDatabaseAnnotation<AutoGenerate>()?.generator?.takeIf { it.isNotBlank() } ?: structure.manager.autoGenerate(column) } }" else "" }
				${ if (type.nullable) "" else "not null" }
			"""
		}

		val keys = structure.getKeys()
		val unique = columns.filter { it.property.hasDatabaseAnnotation<Unique>() }.groupBy { it.property.getDatabaseAnnotation<Unique>()!!.name }

		structure.manager.driver.useHandleUnchecked { it.createUpdate("""
			create table if not exists ${ structure.name } (
				${ columns.joinToString { formatColumn(it) } }
				${ if (keys.isNotEmpty()) ", primary key(${ keys.joinToString { "\"${ it.name }\"${ if (it.property.hasDatabaseAnnotation<AutoIncrement>()) " autoincrement" else "" }" } })" else "" }
				${ if (unique.isNotEmpty()) ", ${ unique.map { "unique(${ it.value.joinToString { "\"${ it.name }\"" } })" }.joinToString() }" else "" }
			)
		""".trim().replace("\\s+".toRegex(), " ")).execute() }

		structure.manager.driver.useHandleUnchecked { it.createQuery("select * from ${ structure.name } limit 1").execute { supplier, _ ->
			val meta = supplier.get().resultSet.metaData
			if (columns.size != meta.columnCount) logger.warn("[${structure.name}] Number of columns in code and database do not match (Code: ${ columns.size }, Database: ${ meta.columnCount })")
			else for (i in 1 .. meta.columnCount) {
				val name = meta.getColumnName(i)
				val column = structure.getColumnFromDatabase(name)

				if (column == null) logger.warn("[${structure.name}] Column $name from database not found in code")
			}
		} }
	}

	override fun <T> createResult(function: () -> T): UpdateResult<T> {
		return try {
			UpdateResult(function(), null, uniqueViolation = false, notNullViolation = false)
		} catch (e: UnableToExecuteStatementException) {
			val sqlException = e.cause as SQLiteException
			val result = UpdateResult<T>(null, sqlException, sqlException.resultCode == SQLiteErrorCode.SQLITE_CONSTRAINT_UNIQUE || sqlException.resultCode == SQLiteErrorCode.SQLITE_CONSTRAINT_PRIMARYKEY, sqlException.resultCode == SQLiteErrorCode.SQLITE_CONSTRAINT_NOTNULL)

			if (!result.uniqueViolation && !result.notNullViolation) throw e
			result
		}
	}
}