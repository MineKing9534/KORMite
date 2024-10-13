package tests.postgres.general

import de.mineking.database.*
import de.mineking.database.vendors.PostgresConnection
import org.junit.jupiter.api.Test
import setup.ConsoleSqlLogger
import setup.UserDao
import setup.recreate
import kotlin.test.assertContains
import kotlin.test.assertEquals

class SelectTest {
	val connection = PostgresConnection("localhost:5432/test", user = "test", password = "test")
	val table = connection.getTable(name = "basic_test") { UserDao() }

	val users = listOf(
		UserDao(name = "Tom", email = "tom@example.com", age = 12),
		UserDao(name = "Alex", email = "alex@example.com", age = 23),
		UserDao(name = "Bob", email = "bob@example.com", age = 50),
		UserDao(name = "Eve", email = "eve@example.com", age = 42),
		UserDao(name = "Max", email = "max@example.com", age = 20)
	)

	init {
		table.recreate()

		users.forEach { table.insert(it) }

		connection.driver.setSqlLogger(ConsoleSqlLogger)
	}

	@Test
	fun rowCount() {
		assertEquals(5, table.selectRowCount())
	}

	@Test
	fun selectAll() {
		assertEquals(5, table.select().list().size)
	}

	@Test
	fun selectBetween() {
		assertEquals(2, table.selectRowCount(where = property("age").isBetween(value(18), value(25))))
		assertEquals(2, table.select(where = property("age").isBetween(value(18), value(25))).list().size)
	}

	@Test
	fun selectSingle() {
		val result = table.select(where = property("name") isEqualTo value("Max")).list()

		assertEquals(1, result.size)

		assertEquals("Max", result.first().name)
		assertEquals(20, result.first().age)
	}

	@Test
	fun selectSpecifiedColumns() {
		val result = table.select("email", "name", where = property("name") isEqualTo value("Max")).list()

		assertEquals(1, result.size)

		assertEquals("Max", result.first().name)
		assertEquals(0, result.first().age) //Age has the default value (0)
	}

	@Test
	fun selectColumn() {
		val result = table.select<Int>(property("age"), where = property("name") isEqualTo value("Max")).list()

		assertEquals(1, result.size)
		assertEquals(20, result.first())
	}

	@Test
	fun selectComplex() {
		assertEquals(42, table.select<Int>(value(42), where = property("name") isEqualTo value("Max")).first())

		assertEquals(21, table.select<Int>(property("age") + " + 1", where = property("name") isEqualTo value("Max")).first())
		assertEquals(40, table.select<Int>(property("age") + " * 2", where = property("name") isEqualTo value("Max")).first())

		assertEquals("MAX", table.select<String>(upperCase(property("name")), where = property("name") isEqualTo value("Max")).first())
	}

	@Test
	fun limit() {
		val result = table.select<String>(property("name"), limit = 2).list()

		assertEquals(2, result.size)
		assertContains(result, "Tom")
		assertContains(result, "Alex")
	}

	@Test
	fun offset() {
		val result = table.select<String>(property("name"), limit = 1, offset = 1).list()

		assertEquals(1, result.size)
		assertContains(result, "Alex")
	}

	@Test
	fun order() {
		val result1 = table.select<String>(property("name"), order = ascendingBy("id")).list()

		assertEquals("Tom", result1[0])
		assertEquals("Max", result1[4])

		val result2 = table.select<String>(property("name"), order = descendingBy("id")).list()

		assertEquals("Max", result2[0])
		assertEquals("Tom", result2[4])

		val result3 = table.select<String>(property("name"), order = ascendingBy("name")).list()

		assertEquals("Alex", result3[0])
		assertEquals("Tom", result3[4])
	}
}