package tests.postgres.specific

import de.mineking.database.*
import org.junit.jupiter.api.Test
import setup.ConsoleSqlLogger
import setup.createConnection
import setup.recreate
import java.util.*
import kotlin.test.assertEquals

data class LocaleTestObject(
	@AutoIncrement @Key @Column val id: Int = 0,
	@Column val locale: Locale = Locale.ENGLISH,
)

class LocaleTest {
	val connection = createConnection()
	val table = connection.getDefaultTable(name = "locale_test") { LocaleTestObject() }

	init {
		table.recreate()

		table.insert(LocaleTestObject(locale = Locale.GERMAN))

		connection.driver.setSqlLogger(ConsoleSqlLogger)
	}

	@Test
	fun selectAll() {
		assertEquals(Locale.GERMAN, table.selectValue(property(LocaleTestObject::locale)).first())
	}
}