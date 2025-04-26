package tests.sqlite.table

import de.mineking.database.*
import org.junit.jupiter.api.Assertions.assertTrue
import setup.*
import kotlin.test.Test
import kotlin.test.assertEquals

interface IdentifiableTable<T: Identifiable> : Table<T> {
    @Select fun getById(@Condition id: Int): T?
}

interface AnnotationTable : IdentifiableTable<UserDao> {
    @Insert fun createUser(@Parameter name: String, @Parameter email: String, @Parameter age: Int): UserDao

    @Query("select :test") fun queryConstant(@Parameter test: List<Int>): QueryResult<List<Int>>
    @Query("select id, email, age, :name from <TABLE>", columns = ["id", "email", "age", "name"]) fun query(@Parameter name: String): List<UserDao>

    @Select fun getAllUsers(): List<UserDao>
    @Select fun getUserByEmail(@Condition email: String): UserDao?
    @Select fun modifiedSelect(): Set<UserDao> = execute<List<UserDao>>().toSet()

    @SelectValue("name") fun selectFirst(@Limit limit: Int, order: Order): List<String>

    @SelectValue("name") fun selectValue(): List<String>
    @SelectValue("name") fun selectSingleValue(@Condition id: Int): String?

    @Update fun update(@Condition id: Int, @Parameter name: String): Int

    @UpdateReturning fun updateReturningFull(@Condition id: Int, @Parameter name: String): UserDao
    @UpdateReturning("name") fun updateReturningName(@Condition id: Int, @Parameter name: String): String
    @UpdateReturning("email") fun updateReturningEmail(@Condition id: Int, @Parameter email: String): Result<String?>

    @Delete fun deleteUser(@Condition email: String): Int
    @Delete fun deleteUser(@Condition id: Int): Boolean
}

class AnnotationTableTest {
    val connection = createConnection()
    val table = connection.getTable<_, AnnotationTable>(name = "basic_test") { UserDao() }

    val users = listOf(
        UserDao(name = "Tom", email = "tom@example.com", age = 12),
        UserDao(name = "Alex", email = "alex@example.com", age = 23),
        UserDao(name = "Bob", email = "bob@example.com", age = 50),
        UserDao(name = "Eve", email = "eve@example.com", age = 42),
        UserDao(name = "Max", email = "max@example.com", age = 20)
    )

    init {
        table.recreate()

        users.forEach { table.implementation.insert(it) }

        connection.driver.setSqlLogger(ConsoleSqlLogger)
    }

    @Test
    fun create() {
        assertEquals(6, table.createUser(name = "Test", email = "test@example.com", age = 0).id)
    }

    @Test
    fun constantQuery() {
        val list = listOf(0, 1, 2)
        assertEquals(list, table.queryConstant(list).first())
    }

    @Test
    fun query() {
        val result = table.query("test")
        assertEquals(5, result.size)
        result.forEach { assertEquals("test", it.name) }
    }

    @Test
    fun selectAll() {
        assertEquals(5, table.getAllUsers().size)
    }

    @Test
    fun getById() {
        assertEquals("Tom", table.getById(1)?.name)
        assertEquals(null, table.getById(6))
    }

    @Test
    fun getByEmail() {
        assertEquals("Tom", table.getUserByEmail("tom@example.com")?.name)
    }

    @Test
    fun modifiedSelect() {
        assertEquals(table.getAllUsers().toSet(), table.modifiedSelect())
    }

    @Test
    fun selectValue() {
        assertEquals(listOf("Tom", "Alex", "Bob", "Eve", "Max"), table.selectValue())

        assertEquals("Tom", table.selectSingleValue(1))
        assertEquals(null, table.selectSingleValue(0))
    }

    @Test
    fun selectFirst() {
        assertEquals(listOf("Tom", "Max"), table.selectFirst(2, order = ascendingBy(UserDao::age)))
    }

    @Test
    fun update() {
        assertEquals(0, table.update(0, "Test"))
        assertEquals(1, table.update(1, "Test"))

        assertEquals("Test", table.getUserByEmail("tom@example.com")?.name)
    }

    @Test
    fun updateReturning() {
        assertEquals(users[0].copy(name = "Test1"), table.updateReturningFull(1, "Test1"))
        assertEquals("Test2", table.updateReturningName(1, "Test2"))
    }

    @Test
    fun updateReturningError() {
        assertEquals("test@example.com", table.updateReturningEmail(1, "test@example.com").getOrThrow())
        assertTrue(table.updateReturningEmail(2, "test@example.com").isError())

        assertEquals(null, table.updateReturningEmail(0, "test@example.com").getOrThrow())
    }

    @Test
    fun delete() {
        assertEquals(1, table.deleteUser("alex@example.com"))
        assertTrue(table.deleteUser(1))

        assertEquals(3, table.implementation.selectRowCount())
    }
}