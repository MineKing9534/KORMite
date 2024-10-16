package tests.sqlite.table

import de.mineking.database.AutoGenerate
import de.mineking.database.Column
import de.mineking.database.Key
import de.mineking.database.Unique
import de.mineking.database.vendors.SQLiteConnection
import org.junit.jupiter.api.Test
import setup.ConsoleSqlLogger
import setup.recreate
import kotlin.test.assertTrue

data class UniqueDao(
    @AutoGenerate @Key @Column val id: Int = 0,
    @Unique("a") @Column val a: String = "",
    @Unique("b") @Column val b: String = ""
)

class UniqueTest {
    val connection = SQLiteConnection("test.db")
    val table = connection.getTable(name = "complex_unique_test") { UniqueDao() }

    init {
        table.recreate()

        table.insert(UniqueDao(a = "a", b = "b"))

        connection.driver.setSqlLogger(ConsoleSqlLogger)
    }

    @Test
    fun simpleUnique() {
        assertTrue(table.insert(UniqueDao(a = "b", b = "a")).isSuccess())

        val result1 = table.insert(UniqueDao(a = "a", b = "a"))
        assertTrue(result1.isError())
        assertTrue(result1.uniqueViolation)

        val result2 = table.insert(UniqueDao(a = "b", b = "b"))
        assertTrue(result2.isError())
        assertTrue(result2.uniqueViolation)
    }
}