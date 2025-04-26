package tests.postgres.basic

import org.junit.jupiter.api.Test
import setup.ConsoleSqlLogger
import setup.User
import setup.createConnection
import setup.recreate
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class InsertTest {
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
    fun checkIds() {
        assertContentEquals(1..5, users.map { it.id })
    }

    @Test
    fun insert() {
        val obj = User(name = "Test", email = "test@example.com", age = 50)
        val result = table.insert(obj)

        assertTrue(result.isSuccess())
        assertEquals(obj, result.value)
        assertEquals(6, obj.id)
    }

    @Test
    fun insertCollision() {
        fun checkResult(obj: User) {
            val old = obj.copy()
            val result = table.insert(obj)

            assertTrue(result.isError())
            assertTrue(result.uniqueViolation)

            assertEquals(old.id, obj.id)
        }

        checkResult(User(name = "Test", email = "tom@example.com", age = 50))
        checkResult(User(id = 1, name = "Test", email = "test@example.com", age = 50))
    }
}