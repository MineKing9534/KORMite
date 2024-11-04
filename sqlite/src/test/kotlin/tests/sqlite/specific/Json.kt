package tests.sqlite.specific

import de.mineking.database.*
import de.mineking.database.vendors.sqlite.SQLiteConnection
import org.junit.jupiter.api.Test
import setup.ConsoleSqlLogger
import setup.recreate
import kotlin.test.assertEquals

data class JsonDao(
	@AutoIncrement @Key @Column val id: Int = 0,
	@Json @Column val map1: LinkedHashMap<String, String> = linkedMapOf(),
	@Json(binary = true) @Column val map2: LinkedHashMap<String, String> = linkedMapOf()
)

class JsonTest {
	val connection = SQLiteConnection("test.db")
	val table = connection.getTable(name = "json_test") { JsonDao() }

	init {
		table.recreate()

		table.insert(JsonDao(map1 = linkedMapOf("a" to "b", "b" to "a"), map2 = linkedMapOf("a" to "b", "b" to "a")))

		connection.driver.setSqlLogger(ConsoleSqlLogger)
	}

	@Test
	fun selectAll() {
		val result = table.select().first()

		assertEquals(linkedMapOf("a" to "b", "b" to "a"), result.map1)
		assertEquals(linkedMapOf("a" to "b", "b" to "a"), result.map2)
	}

	@Test
	fun selectSingle() {
		assertEquals(linkedMapOf("a" to "b", "b" to "a"), table.selectValue(property(JsonDao::map1)).first())
	}
}