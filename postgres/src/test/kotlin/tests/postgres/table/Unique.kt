package tests.postgres.table

import de.mineking.database.AutoIncrement
import de.mineking.database.Column
import de.mineking.database.Key
import de.mineking.database.Unique
import org.junit.jupiter.api.Test
import setup.ConsoleSqlLogger
import setup.createConnection
import setup.recreate
import kotlin.test.assertTrue

data class UniqueTestObject(
    @AutoIncrement @Key @Column val id: Int = 0,
    @Unique @Column val a: String = "",
    @Unique(name = "test") @Column val b: String = "",
    @Unique(name = "test") @Column val c: String = ""
)

class UniqueTest {
    val connection = createConnection()
    val table = connection.getDefaultTable(name = "complex_unique_test") { UniqueTestObject() }

    init {
        table.recreate()

        table.insert(UniqueTestObject(a = "a", b = "a", c = "a"))

        connection.driver.setSqlLogger(ConsoleSqlLogger)
    }

    @Test
    fun simpleUnique() {
        assertTrue(table.insert(UniqueTestObject(a = "b", b = "_", c = "_")).isSuccess())

        val result = table.insert(UniqueTestObject(a = "a", b = ".", c = "."))
        assertTrue(result.isError())
        assertTrue(result.uniqueViolation)
    }

    @Test
    fun complexUnique() {
        assertTrue(table.insert(UniqueTestObject(a = "_", b = "a", c = "b")).isSuccess())
        assertTrue(table.insert(UniqueTestObject(a = ".", b = "b", c = "a")).isSuccess())

        val result = table.insert(UniqueTestObject(a = ",", b = "a", c = "a"))
        assertTrue(result.isError())
        assertTrue(result.uniqueViolation)
    }
}