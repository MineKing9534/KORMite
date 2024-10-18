package tests.sqlite.specific

import de.mineking.database.AutoIncrement
import de.mineking.database.Column
import de.mineking.database.Key
import de.mineking.database.vendors.SQLiteConnection
import org.junit.jupiter.api.Test
import setup.ConsoleSqlLogger
import setup.recreate
import kotlin.test.assertEquals

data class NumericDao(
	@AutoIncrement @Key @Column val id: Int = 0,
	@Column val short: Short = 0,
	@Column val int: Int = 0,
	@Column val long: Long = 0,
	@Column val float: Float = 0F,
	@Column val double: Double = 0.0,
)

class NumericTest {
	val connection = SQLiteConnection("test.db")
	val table = connection.getTable(name = "numeric_test") { NumericDao() }

	init {
		table.recreate()

		table.insert(NumericDao(short = Short.MAX_VALUE, int = Int.MAX_VALUE, long = Long.MAX_VALUE, float = Float.MAX_VALUE, double = Double.MAX_VALUE))

		connection.driver.setSqlLogger(ConsoleSqlLogger)
	}

	@Test
	fun select() {
		//The main purpose here is to verify that reading works without exceptions because of type problems
		val result = table.select().first()
		assertEquals(Double.MAX_VALUE, result.double)
	}
}