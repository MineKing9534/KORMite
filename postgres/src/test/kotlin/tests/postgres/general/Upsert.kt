package tests.postgres.general

import de.mineking.database.*
import de.mineking.database.vendors.postgres.PostgresConnection
import org.junit.jupiter.api.Test
import setup.ConsoleSqlLogger
import setup.recreate
import kotlin.test.assertEquals
import kotlin.test.assertTrue

data class UpsertDao(
	@AutoIncrement @Key @Column val id1: Int = 0,
	@AutoIncrement @Key @Column val id2: Int = 0,
	@Column val name: String = "",
	@Unique @Column val email: String = ""
)

class UpsertTest {
	val connection = PostgresConnection("localhost:5432/test", user = "test", password = "test")
	val table = connection.getTable(name = "upsert_test") { UpsertDao() }

	val entries = listOf(
		UpsertDao(name = "Tom", email = "tom@example.com"),
		UpsertDao(name = "Alex", email = "alex@example.com"),
		UpsertDao(name = "Bob", email = "bob@example.com"),
		UpsertDao(name = "Eve", email = "eve@example.com"),
		UpsertDao(name = "Max", email = "max@example.com")
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
		assertEquals("Test", table.select<String>(property("name"), where = property("id1") isEqualTo value(1)).first())
	}

	@Test
	fun notUpdated() {
		assertTrue(table.upsert(entries[0].copy(email = "alex@example.com")).isError())
		assertEquals("tom@example.com", table.select<String>(property("email"), where = property("id1") isEqualTo value(1)).first())
	}
}