package setup

import de.mineking.database.Table
import mu.KotlinLogging
import org.jdbi.v3.core.statement.SqlLogger
import org.jdbi.v3.core.statement.StatementContext

object ConsoleSqlLogger : SqlLogger {
	val logger = KotlinLogging.logger {}

	override fun logBeforeExecution(context: StatementContext?) {
		logger.info(context!!.renderedSql)
		logger.info(context.binding.toString())

		println()
	}
}

fun Table<*>.recreate() {
	createTable()
	dropTable()
	createTable()
}