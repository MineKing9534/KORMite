package tests.postgres.specific

import de.mineking.database.*
import de.mineking.database.vendors.postgres.contains
import de.mineking.database.vendors.postgres.get
import de.mineking.database.vendors.postgres.size
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Test
import setup.ConsoleSqlLogger
import setup.createConnection
import setup.recreate
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertTrue

data class ArrayDao(
	@AutoIncrement @Key @Column val id: Int = 0,
	@Column val a: Int = 0,
	@Column val intList: List<Int> = listOf(), //Just to ensure correct io for this
	@Column val stringList: List<String> = emptyList(),
	@Column val arrayList: List<Array<String>> = emptyList()
)

class ArrayTest {
	val connection = createConnection()
	val table = connection.getTable(name = "array_test") { ArrayDao() }

	init {
		table.recreate()

		table.insert(ArrayDao(
			a = 0,
			intList = listOf(1, 2, 3),
			stringList = listOf("a", "b", "c"),
			arrayList = listOf(arrayOf("a", "b"), arrayOf("c", "d"), arrayOf("e"))
		))

		table.insert(ArrayDao(
			a = 5,
			intList = listOf(1, 2, 3),
			stringList = listOf("d", "e", "f"),
			arrayList = emptyList()
		))

		connection.driver.setSqlLogger(ConsoleSqlLogger)
	}

	@Test
	fun selectAll() {
		val result = table.select().first()

		assertEquals(3, result.stringList.size)
		assertContentEquals(listOf("a", "b", "c"), result.stringList)

		assertEquals(3, result.arrayList.size)
		assertArrayEquals(arrayOf(arrayOf("a", "b"), arrayOf("c", "d"), arrayOf("e")), result.arrayList.toTypedArray())
	}

	@Test
	fun selectContains() {
		assertEquals(1, table.selectRowCount(where = property(ArrayDao::stringList) contains value("c")))

		assertEquals(1, table.selectRowCount(where = value(listOf(1, 5)) contains property(ArrayDao::a)))
		assertEquals(2, table.selectRowCount(where = value(listOf(0, 5)) contains property(ArrayDao::a)))
	}

	@Test
	fun selectLength() {
		assertEquals(3, table.selectValue(property(ArrayDao::stringList).size).first())
		assertEquals(3, table.selectValue(property(ArrayDao::arrayList).size).first())
	}

	@Test
	fun selectIndex() {
		assertEquals("a", table.selectValue(property(ArrayDao::stringList)[property(ArrayDao::a)]).first())
		assertEquals("a", table.selectValue(property<String>("stringList[a]")).first())

		assertEquals("b", table.selectValue(property(ArrayDao::stringList)[1]).first())
	}

	@Test
	fun indexCondition() {
		assertEquals(1, table.selectRowCount(where = property(ArrayDao::stringList)[0] isEqualTo value("a")))
	}

	@Test
	fun updateCondition() {
		assertTrue(table.update(property(ArrayDao::stringList)[0] to value("e")).isSuccess())
		assertEquals(2, table.selectRowCount(where = property(ArrayDao::stringList)[0] isEqualTo value("e")))
	}
}