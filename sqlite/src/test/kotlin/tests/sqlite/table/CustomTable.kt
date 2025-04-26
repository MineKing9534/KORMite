package tests.sqlite.table

import de.mineking.database.Table
import de.mineking.database.isEqualTo
import de.mineking.database.property
import de.mineking.database.value
import setup.ConsoleSqlLogger
import setup.User
import setup.createConnection
import setup.recreate
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

interface UserTable : Table<User> {
    fun createUser(name: String, email: String, age: Int): User = implementation.insert(User(name = name, email = email, age = age)).getOrThrow()
    fun getUserByEmail(email: String): User? = implementation.select(where = property(User::email) isEqualTo value(email)).firstOrNull()

    fun updateName(email: String, name: String) = (implementation.update(property(User::name) to value(name), where = property(User::email) isEqualTo value(email)).value ?: 0) > 0
}

class CustomTableTest {
    val connection = createConnection()
    val table = connection.getTable<_, UserTable>(name = "basic_test") { User() }

    init {
        table.recreate()

        table.createUser(name = "Tom", email = "tom@example.com", age = 12)
        table.createUser(name = "Alex", email = "alex@example.com", age = 23)
        table.createUser(name = "Bob", email = "bob@example.com", age = 50)
        table.createUser(name = "Eve", email = "eve@example.com", age = 42)
        table.createUser(name = "Max", email = "max@example.com", age = 20)

        connection.driver.setSqlLogger(ConsoleSqlLogger)
    }

    @Test
    fun getByEmail() {
        assertEquals("Tom", table.getUserByEmail("tom@example.com")?.name)
    }

    @Test
    fun updateName() {
        assertTrue(table.updateName("tom@example.com", "Test"))
        assertEquals("Test", table.getUserByEmail("tom@example.com")?.name)
    }

    @Test
    fun notUpdated() {
        assertFalse(table.updateName("test@example.com", "Test"))
    }
}