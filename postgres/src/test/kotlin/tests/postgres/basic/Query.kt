package tests.postgres.basic

import de.mineking.database.value
import de.mineking.database.withContext
import org.junit.jupiter.api.Test
import setup.ConsoleSqlLogger
import setup.UserDao
import setup.createConnection
import setup.recreate
import kotlin.test.assertEquals

class QueryTest {
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
    fun constantQuery() {
        val list = listOf(0, 1, 2)
        assertEquals(list, table.implementation.query<List<Int>>("select :array", mapOf("array" to list.toTypedArray())).first())
    }

    @Test
    fun query() {
        val result = table.implementation.query()
            .defaultNodes()
            .nodes(value("test").withContext(UserDao::name)) //Override name column
            .list()

        assertEquals(5, result.size)
        result.forEach { assertEquals("test", it.name) }
    }
}