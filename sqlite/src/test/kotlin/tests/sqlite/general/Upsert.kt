package tests.sqlite.general

import de.mineking.database.*
import org.junit.jupiter.api.Test
import setup.ConsoleSqlLogger
import setup.createConnection
import setup.recreate
import kotlin.test.assertEquals
import kotlin.test.assertTrue

data class UpsertDao(
	@AutoIncrement @Key @Column val id1: Int = 0,
	//SQLite doesn't support autoincrement with complex keys for some reason...
	@Column val id2: Int = 0,
	@Column val name: String = "",
	@Unique @Column val email: String = ""
)

class UpsertTest {
	val connection = createConnection()
	val table = connection.getDefaultTable(name = "upsert_test") { UpsertDao() }

	val entries = listOf(
		UpsertDao(id2 = 1, name = "Tom", email = "tom@example.com"),
		UpsertDao(id2 = 2, name = "Alex", email = "alex@example.com"),
		UpsertDao(id2 = 3, name = "Bob", email = "bob@example.com"),
		UpsertDao(id2 = 4, name = "Eve", email = "eve@example.com"),
		UpsertDao(id2 = 5, name = "Max", email = "max@example.com")
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
		assertEquals("Test", table.selectValue(property(UpsertDao::name), where = property(UpsertDao::id1) isEqualTo value(1)).first())
	}

	@Test
	fun notUpdated() {
		assertTrue(table.upsert(entries[0].copy(email = "alex@example.com")).isError())
		assertEquals("tom@example.com", table.selectValue(property(UpsertDao::email), where = property(UpsertDao::id1) isEqualTo value(1)).first())
	}
}