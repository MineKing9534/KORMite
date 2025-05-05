package tests.postgres.specific

import de.mineking.database.*
import de.mineking.database.vendors.postgres.JSON
import de.mineking.database.vendors.postgres.json
import org.junit.jupiter.api.Test
import setup.ConsoleSqlLogger
import setup.createConnection
import setup.recreate
import kotlin.test.assertEquals

data class JsonTestObject(
	@AutoIncrement @Key @Column val id: Int = 0,
	@Json @Column val map1: LinkedHashMap<String, String> = linkedMapOf(),
	@Json(binary = true) @Column val map2: LinkedHashMap<String, List<Int>> = linkedMapOf()
)

class JsonTest {
	val connection = createConnection().also {
		it.typeMappers += JSON
	}
	val table = connection.getDefaultTable(name = "json_test") { JsonTestObject() }

	init {
		table.recreate()

		table.insert(JsonTestObject(map1 = linkedMapOf("a" to "b", "b" to "a"), map2 = linkedMapOf("a" to listOf(1), "b" to listOf(2))))

		connection.driver.setSqlLogger(ConsoleSqlLogger)
	}

	@Test
	fun selectAll() {
		val result = table.select().first()

		assertEquals(linkedMapOf("a" to "b", "b" to "a"), result.map1)
		assertEquals(linkedMapOf("a" to listOf(1), "b" to listOf(2)), result.map2)
	}

	@Test
	fun selectSingle() {
		assertEquals(linkedMapOf("a" to "b", "b" to "a"), table.selectValue(property(JsonTestObject::map1)).first())
	}

	@Test
	fun selectChild() {
		assertEquals("b", table.selectValue(property(JsonTestObject::map1).json<String>("a")).first())
		assertEquals(listOf(1), table.selectValue(property(JsonTestObject::map2).json<List<Int>>("a")).first())
	}

	@Test
	fun update() {
		table.update(property(JsonTestObject::map1).json<String>("a") to value("c"))
		assertEquals("c", table.selectValue(property(JsonTestObject::map1).json<String>("a")).first())
	}
}