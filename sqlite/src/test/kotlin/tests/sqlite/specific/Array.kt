package tests.sqlite.specific

import de.mineking.database.AutoIncrement
import de.mineking.database.Column
import de.mineking.database.Key
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Test
import setup.ConsoleSqlLogger
import setup.createConnection
import setup.recreate
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals

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
			stringList = listOf("a", "b", "c"),
			arrayList = listOf(arrayOf("a", "b"), arrayOf("c", "d"), arrayOf("e"))
		))

		table.insert(ArrayDao(
			a = 5,
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
}