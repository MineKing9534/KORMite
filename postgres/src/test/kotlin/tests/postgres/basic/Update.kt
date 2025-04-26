package tests.postgres.basic

import de.mineking.database.*
import org.junit.jupiter.api.Test
import setup.ConsoleSqlLogger
import setup.User
import setup.createConnection
import setup.recreate
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class UpdateTest {
    val connection = createConnection()
    val table = connection.getDefaultTable(name = "basic_test") { User() }

    val users = listOf(
        User(name = "Tom", email = "tom@example.com", age = 12),
        User(name = "Alex", email = "alex@example.com", age = 23),
        User(name = "Bob", email = "bob@example.com", age = 50),
        User(name = "Eve", email = "eve@example.com", age = 42),
        User(name = "Max", email = "max@example.com", age = 20)
    )

    init {
        table.recreate()

        users.forEach { table.insert(it) }

        connection.driver.setSqlLogger(ConsoleSqlLogger)
    }

    @Test
    fun updateName() {
        assertEquals(1, table.update(property(User::name) to value("Test"), where = property(User::id) isEqualTo value(1)).value)
        assertEquals("Test", table.selectValue(property(User::name), where = property(User::id) isEqualTo value(1)).first())
    }

    @Test
    fun updateConflict() {
        val result = table.update(property(User::email) to value("max@example.com"), where = property(User::id) isEqualTo value(1))

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

    @Test
    fun updateReturning() {
        val result = table.updateReturning(property(User::name) to value("Test"))
        assertEquals(users.map { it.copy(name = "Test") }, result.list().getOrThrow())
    }

    @Test
    fun updateReturningSingle() {
        val result = table.updateReturning(property(User::name) to value("Test"), where = property(User::id) isEqualTo value(1), returning = property(User::name))

        val list = result.list().getOrThrow()
        assertEquals(1, list.size)
        assertEquals(listOf("Test"), list)
    }

    @Test
    fun updateReturningConflict() {
        val result = table.updateReturning(property(User::email) to value("test@example.com"))

        assertTrue(result.first().isError())
        assertEquals(users, table.select().list())
    }
}