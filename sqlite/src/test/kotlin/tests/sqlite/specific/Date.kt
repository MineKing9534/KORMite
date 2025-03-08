package tests.sqlite.specific

import de.mineking.database.*
import org.junit.jupiter.api.Test
import setup.ConsoleSqlLogger
import setup.createConnection
import setup.recreate
import java.time.Instant
import java.time.LocalDate
import java.time.temporal.ChronoUnit
import kotlin.test.assertEquals

data class DateDao(
	@AutoIncrement @Key @Column val id: Int = 0,
	@Column val time: Instant = Instant.MIN,
	@Column val date: LocalDate = LocalDate.MIN,
)

class DateTest {
	val connection = createConnection()
	val table = connection.getDefaultTable(name = "date_test") { DateDao() }

	val time = Instant.now()
	val date = LocalDate.now()

	init {
		table.recreate()

		table.insert(DateDao(time = time, date = date))

		connection.driver.setSqlLogger(ConsoleSqlLogger)
	}

	@Test
	fun selectAll() {
		assertEquals(time.truncatedTo(ChronoUnit.MILLIS), table.selectValue(property(DateDao::time)).first().truncatedTo(ChronoUnit.MILLIS))
		assertEquals(date, table.selectValue(property(DateDao::date)).first())
	}
}