package tests.postgres.specific

import de.mineking.database.*
import org.junit.jupiter.api.Test
import setup.ConsoleSqlLogger
import setup.createConnection
import setup.recreate
import java.awt.Color
import kotlin.test.assertEquals

data class ColorDao(
	@AutoIncrement @Key @Column val id: Int = 0,
	@Column val color: Color = Color.WHITE
)

class ColorTest {
	val connection = createConnection()
	val table = connection.getTable(name = "color_test") { ColorDao() }

	init {
		table.recreate()

		table.insert(ColorDao(color = Color.GREEN))

		connection.driver.setSqlLogger(ConsoleSqlLogger)
	}

	@Test
	fun selectAll() {
		assertEquals(Color.GREEN, table.selectValue(property(ColorDao::color)).first())
	}
}