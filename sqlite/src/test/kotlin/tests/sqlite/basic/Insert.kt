package tests.sqlite.basic

import org.junit.jupiter.api.Test
import setup.ConsoleSqlLogger
import setup.UserDao
import setup.createConnection
import setup.recreate
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class InsertTest {
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
    fun checkIds() {
        assertContentEquals(1..5, users.map { it.id })
    }

    @Test
    fun insert() {
        val obj = UserDao(name = "Test", email = "test@example.com", age = 50)
        val result = table.insert(obj)

        result.error?.printStackTrace()

        assertTrue(result.isSuccess())
        assertEquals(obj, result.value)
        assertEquals(6, obj.id)
    }

    @Test
    fun insertCollision() {
        fun checkResult(obj: UserDao) {
            val old = obj.copy()
            val result = table.insert(obj)

            assertTrue(result.isError())
            assertTrue(result.uniqueViolation)

            assertEquals(old.id, obj.id)
        }

        checkResult(UserDao(name = "Test", email = "tom@example.com", age = 50))
        checkResult(UserDao(id = 1, name = "Test", email = "test@example.com", age = 50))
    }
}