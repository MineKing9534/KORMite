package tests.postgres.table

import de.mineking.database.*
import setup.ConsoleSqlLogger
import setup.UserDao
import setup.createConnection
import setup.recreate
import kotlin.test.Test
import kotlin.test.assertEquals

interface AnnotationTable : Table<UserDao> {
    @Select
    fun getAllUsers(): List<UserDao>

    @Select
    fun getUserByEmail(@Condition email: String): UserDao?

    @Insert
    fun createUser(@Parameter name: String, @Parameter email: String, @Parameter age: Int): UserDao

    @Delete
    fun deleteUser(@Condition email: String): Int

    @Delete
    fun deleteUser(@Condition id: Int): Int

    @Update
    fun update(@Condition id: Int, @Parameter name: String): Int
}

class AnnotationTableTest {
    val connection = createConnection()
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

    @Test
    fun update() {
        assertEquals(0, table.update(0, "Test"))
        assertEquals(1, table.update(1, "Test"))

        assertEquals("Test", table.getUserByEmail("tom@example.com")?.name)
    }
}