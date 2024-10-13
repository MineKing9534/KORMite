package tests.postgres.specific

import de.mineking.database.*
import de.mineking.database.vendors.PostgresConnection
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Test
import setup.ConsoleSqlLogger
import setup.recreate
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertTrue

data class ArrayDao(
	@AutoGenerate @Key @Column val id: Int = 0,
	@Column val a: Int = 0,
	@Column val stringList: List<String> = emptyList(),
	@Column val arrayList: Array<List<String>> = emptyArray()
)

class ArrayTest {
	val connection = PostgresConnection("localhost:5432/test", user = "test", password = "test")
	val table = connection.getTable(name = "array_test") { ArrayDao() }

	init {
		table.recreate()

		table.insert(ArrayDao(
			a = 0,
			stringList = listOf("a", "b", "c"),
			arrayList = arrayOf(listOf("a", "b"), listOf("c", "d"), listOf("e"))
		))

		table.insert(ArrayDao(
			a = 5,
			stringList = listOf("d", "e", "f"),
			arrayList = emptyArray()
		))

		connection.driver.setSqlLogger(ConsoleSqlLogger)
	}

	@Test
	fun selectAll() {
		val result = table.select().first()

		assertEquals(3, result.stringList.size)
		assertContentEquals(listOf("a", "b", "c"), result.stringList)

		assertEquals(3, result.arrayList.size)
		assertArrayEquals(arrayOf(listOf("a", "b"), listOf("c", "d"), listOf("e")), result.arrayList)
	}

	@Test
	fun selectContains() {
		assertEquals(1, table.selectRowCount(where = property("stringList") contains value("c")))

		assertEquals(1, table.selectRowCount(where = value(arrayOf(1, 5)) contains property("a")))
		assertEquals(2, table.selectRowCount(where = value(arrayOf(0, 5)) contains property("a")))
	}

	@Test
	fun selectIndex() {
		assertEquals("b", table.select<String>(property("stringList[1]")).first())
		assertEquals("a", table.select<String>(property("stringList[a]")).first())
	}

	@Test
	fun indexCondition() {
		assertEquals(1, table.selectRowCount(where = property("stringList[0]") isEqualTo value("a")))
	}

	@Test
	fun updateCondition() {
		assertTrue(table.update("stringList[0]", value("e")).isSuccess())
		assertEquals(2, table.selectRowCount(where = property("stringList[0]") isEqualTo value("e")))
	}
}