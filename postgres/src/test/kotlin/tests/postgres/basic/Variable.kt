package tests.postgres.basic

import de.mineking.database.*
import org.junit.jupiter.api.Test
import setup.ConsoleSqlLogger
import setup.UserDao
import setup.createConnection
import setup.recreate
import kotlin.test.assertEquals

private val VARIABLE = variable<Int>("var")

//No SQLite test because SQLite does not support lateral joins
class VariableTest {
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
    fun select() {
        val result = table.implementation.query()
            .defaultNodes()
            .nodes(VARIABLE.withContext(UserDao::age))
            .variables(VARIABLE bindTo property(UserDao::age) + " + " + value(1))
            .where(VARIABLE isEqualTo value(21))
            .list()

        assertEquals(1, result.size)
        assertEquals(result[0].age, 21)
    }
}