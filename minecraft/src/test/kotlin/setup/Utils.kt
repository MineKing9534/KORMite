package setup

import de.mineking.database.Table
import de.mineking.database.vendors.postgres.PostgresConnection
import mu.KotlinLogging
import org.jdbi.v3.core.statement.SqlLogger
import org.jdbi.v3.core.statement.StatementContext

fun createConnection() = PostgresConnection("localhost:5432/test", user = "test", password = "test")

object ConsoleSqlLogger : SqlLogger {
	private val logger = KotlinLogging.logger {}

	override fun logBeforeExecution(context: StatementContext?) {
		logger.info(context!!.renderedSql)
		logger.info(context.binding.toString())

		println()
	}
}

fun Table<*>.recreate() {
	implementation.createTable()
	implementation.dropTable()
	implementation.createTable()
}