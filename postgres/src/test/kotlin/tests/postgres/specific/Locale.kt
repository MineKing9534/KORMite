package tests.postgres.specific

import de.mineking.database.*
import de.mineking.database.vendors.postgres.PostgresConnection
import org.junit.jupiter.api.Test
import setup.ConsoleSqlLogger
import setup.recreate
import java.util.*
import kotlin.test.assertEquals

data class LocaleDao(
	@AutoIncrement @Key @Column val id: Int = 0,
	@Column val locale: Locale = Locale.ENGLISH,
)

class LocaleTest {
	val connection = PostgresConnection("localhost:5432/test", user = "test", password = "test")
	val table = connection.getTable(name = "locale_test") { LocaleDao() }

	init {
		table.recreate()

		table.insert(LocaleDao(locale = Locale.GERMAN))

		connection.driver.setSqlLogger(ConsoleSqlLogger)
	}

	@Test
	fun selectAll() {
		assertEquals(Locale.GERMAN, table.selectValue(property(LocaleDao::locale)).first())
	}
}