package tests.postgres.basic

import de.mineking.database.castTo
import de.mineking.database.property
import de.mineking.database.selectValue
import de.mineking.database.vendors.postgres.PostgresType
import org.jdbi.v3.core.statement.UnableToExecuteStatementException
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import setup.ConsoleSqlLogger
import setup.UserDao
import setup.createConnection
import setup.recreate
import kotlin.test.assertEquals

class CastTest {
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
    fun success() {
        assertEquals("12", table.selectValue(property(UserDao::age).castTo<String>(PostgresType.TEXT)).first())
        assertEquals("12", table.selectValue(property(UserDao::age).castTo<String>()).first())
    }

    @Test
    fun fail() {
        assertThrows<UnableToExecuteStatementException> { table.selectValue(property(UserDao::name).castTo<Int>()).first() }
    }
}