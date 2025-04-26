package tests.postgres.basic

import de.mineking.database.value
import de.mineking.database.withContext
import org.junit.jupiter.api.Test
import setup.ConsoleSqlLogger
import setup.User
import setup.createConnection
import setup.recreate
import kotlin.test.assertEquals

class QueryTest {
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
    fun constantQuery() {
        val list = listOf(0, 1, 2)
        assertEquals(list, table.implementation.query<List<Int>>("select :array", mapOf("array" to list.toTypedArray())).first())
    }

    @Test
    fun query() {
        val result = table.implementation.query()
            .defaultNodes()
            .nodes(value("test").withContext(User::name)) //Override name column
            .list()

        assertEquals(5, result.size)
        result.forEach { assertEquals("test", it.name) }
    }
}