package tests.postgres.specific

import de.mineking.database.AutoIncrement
import de.mineking.database.Column
import de.mineking.database.Key
import org.junit.jupiter.api.Test
import setup.ConsoleSqlLogger
import setup.createConnection
import setup.recreate
import java.math.BigDecimal
import java.math.BigInteger
import kotlin.test.assertEquals

data class NumericDao(
	@AutoIncrement @Key @Column val id: Int = 0,
	@Column val short: Short = 0,
	@Column val int: Int = 0,
	@Column val long: Long = 0,
	@Column val bigInt: BigInteger = BigInteger.ZERO,
	@Column val float: Float = 0F,
	@Column val double: Double = 0.0,
	@Column val bigDecimal: BigDecimal = BigDecimal.ZERO,
)

class NumericTest {
	val connection = createConnection()
	val table = connection.getDefaultTable(name = "numeric_test") { NumericDao() }

	init {
		table.recreate()

		table.insert(NumericDao(short = Short.MAX_VALUE, int = Int.MAX_VALUE, long = Long.MAX_VALUE, bigInt = Long.MAX_VALUE.toBigInteger(), float = Float.MAX_VALUE, double = Double.MAX_VALUE, bigDecimal = BigDecimal("${ Double.MAX_VALUE.toBigDecimal().toPlainString() }0.5")))

		connection.driver.setSqlLogger(ConsoleSqlLogger)
	}

	@Test
	fun select() {
		//The main purpose here is to verify that reading works without exceptions because of type problems
		val result = table.select().first()
		assertEquals(BigDecimal("${ Double.MAX_VALUE.toBigDecimal().toPlainString() }0.5"), result.bigDecimal)
	}
}