package tests.postgres.specific

import de.mineking.database.*
import de.mineking.database.vendors.postgres.PostgresConnection
import org.junit.jupiter.api.Test
import setup.ConsoleSqlLogger
import setup.recreate
import java.awt.Color
import kotlin.test.assertEquals

data class ColorDao(
	@AutoIncrement @Key @Column val id: Int = 0,
	@Column val color: Color = Color.WHITE
)

class ColorTest {
	val connection = PostgresConnection("localhost:5432/test", user = "test", password = "test")
	val table = connection.getTable(name = "color_test") { ColorDao() }

	init {
		table.recreate()

		table.insert(ColorDao(color = Color.GREEN))

		connection.driver.setSqlLogger(ConsoleSqlLogger)
	}

	@Test
	fun selectAll() {
		assertEquals(Color.GREEN, table.select<Color>(property("color")).first())
	}
}