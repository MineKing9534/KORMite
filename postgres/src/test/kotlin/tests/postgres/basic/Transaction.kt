package tests.postgres.basic

import org.junit.jupiter.api.Test
import setup.ConsoleSqlLogger
import setup.User
import setup.createConnection
import setup.recreate
import kotlin.test.assertEquals

class TransactionTest {
	val connection = createConnection()
	val table = connection.getDefaultTable(name = "basic_test") { User() }

	init {
		table.recreate()

		connection.driver.setSqlLogger(ConsoleSqlLogger)
	}

	@Test
	fun default() {
		connection.inTransaction {
			table.insert(User(name = "Tom", email = "tom@example.com", age = 12))
		}

		assertEquals(1, table.selectRowCount())
	}

	@Test
	fun rollback() {
		connection.inTransaction {
			table.insert(User(name = "Tom", email = "tom@example.com", age = 12))
			it.rollback()
		}

		assertEquals(0, table.selectRowCount())
	}
}