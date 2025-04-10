package tests.postgres.basic

import de.mineking.database.*
import org.junit.jupiter.api.Test
import setup.ConsoleSqlLogger
import setup.UserDao
import setup.createConnection
import setup.recreate
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class UpdateTest {
    val connection = createConnection()
    val table = connection.getDefaultTable(name = "basic_test") { UserDao() }

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
        assertEquals(1, table.update(property(UserDao::name) to value("Test"), where = property(UserDao::id) isEqualTo value(1)).value)
        assertEquals("Test", table.selectValue(property(UserDao::name), where = property(UserDao::id) isEqualTo value(1)).first())
    }

    @Test
    fun updateConflict() {
        val result = table.update(property(UserDao::email) to value("max@example.com"), where = property(UserDao::id) isEqualTo value(1))

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
        val result = table.updateReturning(property(UserDao::name) to value("Test"))
        assertEquals(users.map { it.copy(name = "Test") }, result.list().getOrThrow())
    }

    @Test
    fun updateReturningSingle() {
        val result = table.updateReturning(property(UserDao::name) to value("Test"), where = property(UserDao::id) isEqualTo value(1), returning = property(UserDao::name))

        val list = result.list().getOrThrow()
        assertEquals(1, list.size)
        assertEquals(listOf("Test"), list)
    }

    @Test
    fun updateReturningConflict() {
        val result = table.updateReturning(property(UserDao::email) to value("test@example.com"))

        assertTrue(result.first().isError())
        assertEquals(users, table.select().list())
    }
}