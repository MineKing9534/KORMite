package tests.sqlite.basic

import de.mineking.database.isBetween
import de.mineking.database.property
import de.mineking.database.value
import org.junit.jupiter.api.Test
import setup.ConsoleSqlLogger
import setup.User
import setup.createConnection
import setup.recreate
import kotlin.test.assertEquals

class DeleteTest {
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
    fun deleteAll() {
        assertEquals(5, table.delete())
        assertEquals(0, table.selectRowCount())
    }

    @Test
    fun deleteCondition() {
        assertEquals(2, table.delete(where = property(User::age).isBetween(value(18), value(25))))
        assertEquals(3, table.selectRowCount())
    }
}