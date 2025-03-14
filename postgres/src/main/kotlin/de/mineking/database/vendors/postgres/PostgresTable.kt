package de.mineking.database.vendors.postgres

import de.mineking.database.*
import de.mineking.database.vendors.postgres.PostgresConnection.Companion.logger
import org.jdbi.v3.core.kotlin.useHandleUnchecked
import org.jdbi.v3.core.statement.UnableToExecuteStatementException
import org.postgresql.util.PSQLState
import java.sql.SQLException
import kotlin.reflect.KClass

class PostgresTable<T: Any>(
	type: KClass<*>,
	structure: TableStructure<T>,
	instance: () -> T
) : TableImplementation<T>(type, structure, instance) {
	override fun createTable() {
		val columns = structure.getAllColumns()

		fun <C> formatColumn(column: ColumnData<T, C>): String {
			val root = column.getRootColumn()
			return """
				"${ column.name }" ${ column.mapper.getType(column, column.table, column.type).sqlName }
				${ root.property.takeIf { !it.returnType.isMarkedNullable }?.let { " not null" } ?: "" }
				${ if (root.property.hasDatabaseAnnotation<AutoIncrement>()) " generated by default as identity" else "" }
				${ if (root.property.hasDatabaseAnnotation<AutoGenerate>()) " default ${ root.property.getDatabaseAnnotation<AutoGenerate>()?.generator?.takeIf { it.isNotBlank() } ?: structure.manager.autoGenerate(column) } }" else "" }
			"""
		}

		val keys = structure.getKeys()
		val unique = columns.filter { it.getRootColumn().property.hasDatabaseAnnotation<Unique>() }.groupBy { it.getRootColumn().property.getDatabaseAnnotation<Unique>()!!.name }

		structure.manager.driver.useHandleUnchecked { it.createUpdate("""
			create table if not exists ${ structure.name } (
				${ columns.joinToString { formatColumn(it) } }
				${ if (keys.isNotEmpty()) ", primary key (${ keys.joinToString { "\"${it.name}\"" } })" else "" }
				${ if (unique.isNotEmpty()) ", ${ unique.map { "unique (${ it.value.joinToString { "\"${ it.name }\"" } })" }.joinToString() }" else "" }
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
			val sqlException = e.cause as SQLException
			val result = UpdateResult<T>(null, sqlException, sqlException.sqlState == PSQLState.UNIQUE_VIOLATION.state, sqlException.sqlState == PSQLState.NOT_NULL_VIOLATION.state)

			if (!result.uniqueViolation && !result.notNullViolation) throw e
			result
		}
	}
}