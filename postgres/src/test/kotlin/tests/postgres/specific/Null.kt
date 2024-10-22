package tests.postgres.specific

import de.mineking.database.*
import de.mineking.database.vendors.postgres.PostgresConnection
import org.junit.jupiter.api.Test
import setup.ConsoleSqlLogger
import setup.recreate
import kotlin.test.assertEquals
import kotlin.test.assertTrue

data class NullDao(
	@AutoIncrement @Key @Column val id: Int = 0,
	@Column val test: String? = null,
	@Column val name: String = ""
)

class NullTest {
	val connection = PostgresConnection("localhost:5432/test", user = "test", password = "test")
	val table = connection.getTable(name = "null_test") { NullDao() }

	init {
		table.recreate()

		table.insert(NullDao(test = "abc", name = "not-null"))
		table.insert(NullDao(test = null, name = "null"))

		connection.driver.setSqlLogger(ConsoleSqlLogger)
	}

	@Test
	fun selectAll() {
		val result = table.select<String?>(property("test")).list()

		assertEquals(2, result.size)

		assertEquals("abc", result[0])
		assertEquals(null, result[1])
	}

	@Test
	fun selectIsNull() {
		assertEquals("not-null", table.select<String>(property("name"), where = property("test").isNotNull()).first())
		assertEquals("null", table.select<String>(property("name"), where = property("test").isNull()).first())
	}

	@Test
	fun updateNullError() {
		fun checkResult(result: UpdateResult<*>) {
			assertTrue(result.isError())
			assertTrue(result.notNullViolation)
		}

		checkResult(table.update("name", value<String?>(null), where = property("id") isEqualTo value(1)))
		checkResult(table.update("name", nullValue(), where = property("id") isEqualTo value(1)))
	}

	@Test
	fun updateNull() {
		val result = table.update("test", nullValue(), where = property("id") isEqualTo value(1))

		assertTrue(result.isSuccess())
		assertEquals(1, result.value)

		assertEquals(null, table.select<String?>(property("test"), where = property("id") isEqualTo value(1)).first())
	}
}