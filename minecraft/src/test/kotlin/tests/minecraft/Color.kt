package tests.minecraft

import de.mineking.database.*
import de.mineking.database.vendors.postgres.PostgresMappers
import net.kyori.adventure.text.format.NamedTextColor
import net.kyori.adventure.text.format.TextColor
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import setup.ConsoleSqlLogger
import setup.createConnection
import setup.createServer
import setup.recreate

data class ColorDao(
	@AutoIncrement @Key @Column val id: Int = 0,
	@Column val color: TextColor = TextColor.color(0),
	@Column val namedColor: NamedTextColor = NamedTextColor.BLACK
)

class ColorTest {
	val connection = createConnection()
	val table: Table<ColorDao>

	init {
		connection.registerMinecraftMappers(createServer(), PostgresMappers.STRING, PostgresMappers.UUID_MAPPER, PostgresMappers.ARRAY, PostgresMappers.DOUBLE)
		table = connection.getTable(name = "color_test") { ColorDao() }

		table.recreate()

		table.insert(ColorDao(color = TextColor.color(0x00ff00), namedColor = NamedTextColor.GREEN))

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
		assertEquals(TextColor.color(0x00ff00), table.selectValue(property(ColorDao::color), limit = 1).first())
		assertEquals(NamedTextColor.GREEN, table.selectValue(property(ColorDao::namedColor), limit = 1).first())

		assertEquals(1, table.selectRowCount(where = property(ColorDao::color) isEqualTo value(TextColor.color(0x00ff00))))
	}
}