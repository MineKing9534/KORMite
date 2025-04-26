package tests.postgres.specific

import de.mineking.database.*
import org.junit.jupiter.api.Test
import setup.ConsoleSqlLogger
import setup.createConnection
import setup.recreate
import java.awt.Color
import kotlin.test.assertEquals

data class ColorTestObject(
	@AutoIncrement @Key @Column val id: Int = 0,
	@Column val color: Color = Color.WHITE
)

class ColorTest {
	val connection = createConnection()
	val table = connection.getDefaultTable(name = "color_test") { ColorTestObject() }

	init {
		table.recreate()

		table.insert(ColorTestObject(color = Color.GREEN))

		connection.driver.setSqlLogger(ConsoleSqlLogger)
	}

	@Test
	fun selectAll() {
		assertEquals(Color.GREEN, table.selectValue(property(ColorTestObject::color)).first())
	}
}