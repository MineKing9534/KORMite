package tests.minecraft

import de.mineking.database.*
import de.mineking.database.vendors.postgres.PostgresConnection
import de.mineking.database.vendors.postgres.PostgresMappers
import org.bukkit.Location
import org.bukkit.World
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import setup.ConsoleSqlLogger
import setup.createServer
import setup.createWorld
import setup.recreate
import java.util.*

data class LocationDao(
	@AutoIncrement @Key @Column val id: Int = 0,
	@Column val location1: Location = Location(null, 0.0, 0.0, 0.0),
	@LocationWorldColumn(name = "worldTest") @Column val location2: Location = Location(null, 0.0, 0.0, 0.0),
	@Column(name = "world") val worldTest: World = createWorld()
)

class LocationTest {
	val connection = PostgresConnection("localhost:5432/test", user = "test", password = "test")
	val table: Table<LocationDao>

	val id1 = UUID.randomUUID()
	val id2 = UUID.randomUUID()

	val worlds = listOf(
		createWorld(id1),
		createWorld(id2)
	)

	init {
		connection.registerMinecraftMappers(createServer(worlds = worlds), PostgresMappers.STRING, PostgresMappers.UUID_MAPPER, PostgresMappers.ARRAY, PostgresMappers.DOUBLE)
		table = connection.getTable(name = "location_test") { LocationDao() }

		table.recreate()

		table.insert(LocationDao(location1 = Location(worlds[0], 0.0, 5.0, 0.0), location2 = Location(null, 0.0, 0.0, 0.0), worldTest = worlds[1]))

		connection.driver.setSqlLogger(ConsoleSqlLogger)
	}

	@Test
	fun selectAll() {
		val result = table.select().list()

		assertEquals(result.size, 1)
		assertEquals(id1, result.first().location1.world.uid)
		assertEquals(id2, result.first().location2.world.uid)
		assertEquals(id2, result.first().worldTest.uid)
	}

	@Test
	fun coordinates() {
		assertEquals(1, table.selectRowCount(where = property("location1[0]") isEqualTo property("location1.x")))
		assertEquals(0.0, table.select<Double>(property("location1.x")).first())
	}

	@Test
	fun selectColumn() {
		assertEquals(Location(worlds[0], 0.0, 5.0, 0.0), table.select<Location>(property("location1")).first())

		assertEquals(id1, table.select<World>(property("location1.world")).first().uid)

		assertEquals(id2, table.select<World>(property("worldTest")).first().uid)
		assertEquals(id2, table.select<World>(property("location2.world")).first().uid)
	}
}