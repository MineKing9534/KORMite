package tests.sqlite.specific

import de.mineking.database.*
import de.mineking.database.vendors.SQLiteConnection
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
	val connection = SQLiteConnection("test.db")
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
}