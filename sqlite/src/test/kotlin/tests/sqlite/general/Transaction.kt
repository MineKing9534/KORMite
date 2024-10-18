package tests.sqlite.general

import de.mineking.database.vendors.SQLiteConnection
import org.junit.jupiter.api.Test
import setup.ConsoleSqlLogger
import setup.UserDao
import setup.recreate
import kotlin.test.assertEquals

class TransactionTest {
	val connection = SQLiteConnection("test.db")
	val table = connection.getTable(name = "basic_test") { UserDao() }

	init {
		table.recreate()

		connection.driver.setSqlLogger(ConsoleSqlLogger)
	}

	@Test
	fun default() {
		connection.inTransaction {
			table.insert(UserDao(name = "Tom", email = "tom@example.com", age = 12))
		}

		assertEquals(1, table.selectRowCount())
	}

	@Test
	fun rollback() {
		connection.inTransaction {
			table.insert(UserDao(name = "Tom", email = "tom@example.com", age = 12))
			it.rollback()
		}

		assertEquals(0, table.selectRowCount())
	}
}