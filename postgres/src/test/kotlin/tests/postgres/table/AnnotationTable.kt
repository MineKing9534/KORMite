package tests.postgres.table

import de.mineking.database.*
import de.mineking.database.vendors.postgres.PostgresConnection
import setup.ConsoleSqlLogger
import setup.UserDao
import setup.recreate
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

interface AnnotationTable : Table<UserDao> {
    @Select
    fun getAllUsers(): List<UserDao>

    @Select
    fun getUserByEmail(@Parameter email: String): UserDao?

    @Insert
    fun createUser(@Parameter name: String, @Parameter email: String, @Parameter age: Int): UserDao

    @Delete
    fun deleteUser(@Parameter email: String): Int

    @Delete
    fun deleteUser(@Parameter id: Int): Int
}

class AnnotationTableTest {
    val connection = PostgresConnection("localhost:5432/test", user = "test", password = "test")
    val table = connection.getTable<_, AnnotationTable>(name = "basic_test") { UserDao() }

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
    fun create() {
        assertEquals(6, table.createUser(name = "Test", email = "test@example.com", age = 0).id)
    }

    @Test
    fun selectAll() {
        assertEquals(5, table.getAllUsers().size)
    }

    @Test
    fun getByEmail() {
        assertEquals("Tom", table.getUserByEmail("tom@example.com")?.name)
    }

    @Test
    fun delete() {
        assertEquals(1, table.deleteUser("alex@example.com"))
        assertEquals(1, table.deleteUser(1))

        assertEquals(3, table.selectRowCount())
    }
}