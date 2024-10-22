package tests.postgres.general

import de.mineking.database.isEqualTo
import de.mineking.database.property
import de.mineking.database.select
import de.mineking.database.value
import de.mineking.database.vendors.postgres.PostgresConnection
import org.junit.jupiter.api.Test
import setup.ConsoleSqlLogger
import setup.UserDao
import setup.recreate
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class UpdateTest {
    val connection = PostgresConnection("localhost:5432/test", user = "test", password = "test")
    val table = connection.getTable(name = "basic_test") { UserDao() }

    val users = listOf(
        UserDao(name = "Tom", email = "tom@example.com", age = 12),
        UserDao(name = "Alex", email = "alex@example.com", age = 23),
        UserDao(name = "Bob", email = "bob@example.com", age = 50),
        UserDao(name = "Eve", email = "eve@example.com", age = 42),
        UserDao(name = "Max", email = "max@example.com", age = 20)
    )

    init {
        table.recreate()

        users.forEach { table.insert(it) }

        connection.driver.setSqlLogger(ConsoleSqlLogger)
    }

    @Test
    fun updateName() {
        assertEquals(1, table.update("name", value("Test"), where = property("id") isEqualTo value(1)).value)
        assertEquals("Test", table.select<String>(property("name"), where = property("id") isEqualTo value(1)).first())
    }

    @Test
    fun updateConflict() {
        val result = table.update("email", value("max@example.com"), where = property("id") isEqualTo value(1))

        assertTrue(result.isError())
        assertTrue(result.uniqueViolation)
    }

    @Test
    fun updateObject() {
        val firstUser = users[0].copy(name = "Test")
        val result = table.update(firstUser)

        assertTrue(result.isSuccess())
        assertEquals(firstUser, result.value)
    }
}