package tests.sqlite.basic

import de.mineking.database.castTo
import de.mineking.database.property
import de.mineking.database.selectValue
import de.mineking.database.vendors.sqlite.SQLiteType
import org.junit.jupiter.api.Test
import setup.ConsoleSqlLogger
import setup.User
import setup.createConnection
import setup.recreate
import kotlin.test.assertEquals

class CastTest {
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
    fun success() {
        assertEquals("12", table.selectValue(property(User::age).castTo<String>(SQLiteType.TEXT)).first())
        assertEquals("12", table.selectValue(property(User::age).castTo<String>()).first())
    }

    @Test
    fun fail() {
        val result = table.selectValue(property(User::name).castTo<Int>()).first()
        assertEquals(0, result) //SQLite doesn't throw an exception for some reason but uses 0 as fallback integer
    }
}