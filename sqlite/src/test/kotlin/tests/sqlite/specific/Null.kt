package tests.sqlite.specific

import de.mineking.database.*
import org.junit.jupiter.api.Test
import setup.ConsoleSqlLogger
import setup.createConnection
import setup.recreate
import kotlin.test.assertEquals
import kotlin.test.assertTrue

data class NullTestObject(
	@AutoIncrement @Key @Column val id: Int = 0,
	@Column val test: String? = null,
	@Column val name: String = ""
)

class NullTest {
	val connection = createConnection()
	val table = connection.getDefaultTable(name = "null_test") { NullTestObject() }

	init {
		table.recreate()

		table.insert(NullTestObject(test = "abc", name = "not-null"))
		table.insert(NullTestObject(test = null, name = "null"))

		connection.driver.setSqlLogger(ConsoleSqlLogger)
	}

	@Test
	fun selectAll() {
		val result = table.selectValue(property(NullTestObject::test)).list()

		assertEquals(2, result.size)

		assertEquals("abc", result[0])
		assertEquals(null, result[1])
	}

	@Test
	fun selectIsNull() {
		assertEquals("not-null", table.selectValue(property(NullTestObject::name), where = property(NullTestObject::test).isNotNull()).first())
		assertEquals("null", table.selectValue(property(NullTestObject::name), where = property(NullTestObject::test).isNull()).first())
	}

	@Test
	@Suppress("UNCHECKED_CAST")
	fun updateNullError() {
		fun checkResult(result: Result<*>) {
			assertTrue(result.isError())
			assertTrue(result.notNullViolation)
		}

		//Updating to null values with nun-null types will actually cause compile-time type problems without the unchecked casts
		checkResult(table.update(property(NullTestObject::name) to (value<String?>(null) as Node<String>), where = property(NullTestObject::id) isEqualTo value(1)))
		checkResult(table.update(property(NullTestObject::name) to (nullValue<String>() as Node<String>), where = property(NullTestObject::id) isEqualTo value(1)))
	}

	@Test
	fun updateNull() {
		val result = table.update(property(NullTestObject::test) to nullValue(), where = property(NullTestObject::id) isEqualTo value(1))

		assertTrue(result.isSuccess())
		assertEquals(1, result.value)

		assertEquals(null, table.selectValue(property(NullTestObject::test), where = property(NullTestObject::id) isEqualTo value(1)).first())
	}
}