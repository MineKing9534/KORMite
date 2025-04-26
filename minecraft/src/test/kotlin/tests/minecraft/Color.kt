package tests.minecraft

import de.mineking.database.*
import net.kyori.adventure.text.format.NamedTextColor
import net.kyori.adventure.text.format.TextColor
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import setup.ConsoleSqlLogger
import setup.createConnection
import setup.createServer
import setup.recreate

data class ColorTestObject(
	@AutoIncrement @Key @Column val id: Int = 0,
	@Column val color: TextColor = TextColor.color(0),
	@Column val namedColor: NamedTextColor = NamedTextColor.BLACK
)

class ColorTest {
	val connection = createConnection().apply { registerMinecraftMappers(createServer()) }
	val table = connection.getDefaultTable(name = "color_test") { ColorTestObject() }

    init {
		table.recreate()

		table.insert(ColorTestObject(color = TextColor.color(0x00ff00), namedColor = NamedTextColor.GREEN))

		connection.driver.setSqlLogger(ConsoleSqlLogger)
	}

	@Test
	fun selectAll() {
		val result = table.select().list()

		assertEquals(TextColor.color(0x00ff00), result[0].color)
		assertEquals(NamedTextColor.GREEN, result[0].namedColor)
	}

	@Test
	fun selectColumn() {
		assertEquals(TextColor.color(0x00ff00), table.selectValue(property(ColorTestObject::color), limit = 1).first())
		assertEquals(NamedTextColor.GREEN, table.selectValue(property(ColorTestObject::namedColor), limit = 1).first())

		assertEquals(1, table.selectRowCount(where = property(ColorTestObject::color) isEqualTo value(TextColor.color(0x00ff00))))
	}
}