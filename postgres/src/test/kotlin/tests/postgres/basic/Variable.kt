package tests.postgres.basic

import de.mineking.database.*
import org.junit.jupiter.api.Test
import setup.ConsoleSqlLogger
import setup.User
import setup.createConnection
import setup.recreate
import kotlin.test.assertEquals

private val VARIABLE = variable<Int>("var")

//No SQLite test because SQLite does not support lateral joins
class VariableTest {
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
    fun select() {
        val result = table.implementation.query()
            .defaultNodes()
            .nodes(VARIABLE.withContext(User::age))
            .variables(VARIABLE bindTo property(User::age) + " + " + value(1))
            .where(VARIABLE isEqualTo value(21))
            .list()

        assertEquals(1, result.size)
        assertEquals(result[0].age, 21)
    }
}