package tests.postgres.basic

import de.mineking.database.*
import org.junit.jupiter.api.Test
import setup.ConsoleSqlLogger
import setup.createConnection
import setup.recreate
import kotlin.test.assertEquals
import kotlin.test.assertTrue

data class UpsertTestObject(
	@AutoIncrement @Key @Column val id1: Int = 0,
	@AutoIncrement @Key @Column val id2: Int = 0,
	@Column val name: String = "",
	@Unique @Column val email: String = ""
)

class UpsertTest {
	val connection = createConnection()
	val table = connection.getDefaultTable(name = "upsert_test") { UpsertTestObject() }

	val entries = listOf(
		UpsertTestObject(name = "Tom", email = "tom@example.com"),
		UpsertTestObject(name = "Alex", email = "alex@example.com"),
		UpsertTestObject(name = "Bob", email = "bob@example.com"),
		UpsertTestObject(name = "Eve", email = "eve@example.com"),
		UpsertTestObject(name = "Max", email = "max@example.com")
	)

	init {
		table.recreate()

		entries.forEach { table.upsert(it) }

		connection.driver.setSqlLogger(ConsoleSqlLogger)
	}

	@Test
	fun checkInsert() {
		assertEquals(5, table.selectRowCount())
	}

	@Test
	fun update() {
		assertTrue(table.upsert(entries[0].copy(name = "Test")).isSuccess())
		assertEquals("Test", table.selectValue(property(UpsertTestObject::name), where = property(UpsertTestObject::id1) isEqualTo value(1)).first())
	}

	@Test
	fun notUpdated() {
		assertTrue(table.upsert(entries[0].copy(email = "alex@example.com")).isError())
		assertEquals("tom@example.com", table.selectValue(property(UpsertTestObject::email), where = property(UpsertTestObject::id1) isEqualTo value(1)).first())
	}
}