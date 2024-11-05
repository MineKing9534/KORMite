package tests.sqlite.table

import de.mineking.database.Table
import de.mineking.database.isEqualTo
import de.mineking.database.property
import de.mineking.database.value
import de.mineking.database.vendors.sqlite.SQLiteConnection
import setup.ConsoleSqlLogger
import setup.UserDao
import setup.recreate
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

interface UserTable : Table<UserDao> {
    fun createUser(name: String, email: String, age: Int): UserDao = insert(UserDao(name = name, email = email, age = age)).getOrThrow()
    fun getUserByEmail(email: String): UserDao? = select(where = property(UserDao::email) isEqualTo value(email)).findFirst()

    fun updateName(email: String, name: String) = (update(property(UserDao::name) to value(name), where = property(UserDao::email) isEqualTo value(email)).value ?: 0) > 0
}

class CustomTableTest {
    val connection = SQLiteConnection("test.db")
    val table = connection.getTable<_, UserTable>(name = "basic_test") { UserDao() }

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