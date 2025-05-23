package tests.postgres.basic

import de.mineking.database.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import org.junit.jupiter.api.assertThrows
import setup.ConsoleSqlLogger
import setup.User
import setup.createConnection
import setup.recreate
import kotlin.test.assertContains
import kotlin.test.assertEquals

class SelectTest {
	val connection = createConnection()
	val table = connection.getDefaultTable(name = "basic_test") { User() }

	val users = listOf(
		User(name = "Tom", email = "tom@example.com", age = 12),
		User(name = "Alex", email = "alex@example.com", age = 23),
		User(name = "Bob", email = "bob@example.com", age = 50),
		User(name = "Eve", email = "eve@example.com", age = 42),
		User(name = "Max", email = "max@example.com", age = 20)
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
		assertEquals(users, table.select().list())
	}

	@Test
	fun selectOutOfOrder() {
		assertDoesNotThrow {
			table.select(property(User::id), property(User::email), property(User::name), value(""), value(""), property(User::age)).list()
		}
	}

	@Test
	fun first() {
		val result1 = table.select()
		assertEquals(users[0], result1.first())

		val result2 = table.select(where = Conditions.NONE)
		assertThrows<IllegalStateException> { result2.first() }
	}

	@Test
	fun findFirst() {
		val result1 = table.select()
		assertEquals(users[0], result1.firstOrNull())

		val result2 = table.select(where = Conditions.NONE)
		assertEquals(null, result2.firstOrNull())
	}

	@Test
	fun selectBetween() {
		assertEquals(2, table.selectRowCount(where = property(User::age).isBetween(value(18), value(25))))
		assertEquals(2, table.select(where = property(User::age).isBetween(value(18), value(25))).list().size)
	}

	@Test
	fun selectSingle() {
		val result = table.select(where = property(User::name) isEqualTo value("Max")).list()

		assertEquals(1, result.size)

		assertEquals("Max", result.first().name)
		assertEquals(20, result.first().age)
	}

	@Test
	fun selectSpecifiedColumns() {
		val result = table.select(User::email, User::name, where = property(User::name) isEqualTo value("Max")).list()

		assertEquals(1, result.size)

		assertEquals("Max", result.first().name)
		assertEquals(0, result.first().age) //Age has the default value (0)
	}

	@Test
	fun selectComplexSpecifiedColumns() {
		val result = table.select(property(User::name).uppercase(), value(Integer.MAX_VALUE).withContext(User::age), where = property(User::name) isEqualTo value("Max")).list()

		assertEquals(1, result.size)

		assertEquals("MAX", result.first().name)
		assertEquals(Integer.MAX_VALUE, result.first().age) //Age has the default value (0)
	}

	@Test
	fun selectColumn() {
		val result = table.selectValue(property(User::age), where = property(User::name) isEqualTo value("Max")).list()

		assertEquals(1, result.size)
		assertEquals(20, result.first())

		assertEquals(3, table.selectValue(property(User::name).length, where = property(User::name) isEqualTo value("Max")).first())
	}

	@Test
	fun selectComplex() {
		assertEquals(42, table.selectValue(value(42), where = property(User::name) isEqualTo value("Max")).first())

		assertEquals(21, table.selectValue(property(User::age) + " + 1", where = property(User::name) isEqualTo value("Max")).first())
		assertEquals(40, table.selectValue(property(User::age) + " * 2", where = property(User::name) isEqualTo value("Max")).first())

		assertEquals("MAX", table.selectValue(property(User::name).uppercase(), where = property(User::name) isEqualTo value("Max")).first())
	}

	@Test
	fun limit() {
		val result = table.selectValue(property(User::name), limit = 2).list()

		assertEquals(2, result.size)
		assertContains(result, "Tom")
		assertContains(result, "Alex")
	}

	@Test
	fun offset() {
		val result = table.selectValue(property(User::name), limit = 1, offset = 1).list()

		assertEquals(1, result.size)
		assertContains(result, "Alex")
	}

	@Test
	fun order() {
		val result1 = table.selectValue(property(User::name), order = ascendingBy(User::id)).list()

		assertEquals("Tom", result1[0])
		assertEquals("Max", result1[4])

		val result2 = table.selectValue(property(User::name), order = descendingBy(User::id)).list()

		assertEquals("Max", result2[0])
		assertEquals("Tom", result2[4])

		val result3 = table.selectValue(property(User::name), order = ascendingBy(User::name)).list()

		assertEquals("Alex", result3[0])
		assertEquals("Tom", result3[4])
	}
}