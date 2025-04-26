package tests.minecraft

import de.mineking.database.*
import org.bukkit.Location
import org.bukkit.World
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import setup.*
import java.util.*

private val DUMMY_WORLD = createDummy<World>()

data class LocationTestObject(
	@AutoIncrement @Key @Column val id: Int = 0,
	@Column val location1: Location = Location(null, 0.0, 0.0, 0.0),
	@Column val location2: Location = Location(null, 0.0, 0.0, 0.0),
	@Column val world: World = DUMMY_WORLD,
	@SelectAs("location1 ->> 'world'") val locationWorld: World = DUMMY_WORLD
)

class LocationTest {
	val id = UUID.randomUUID()
	val world = createWorld(id)

	val connection = createConnection().apply { registerMinecraftMappers(createServer(worlds = listOf(world))) }
	val table = connection.getDefaultTable(name = "location_test") { LocationTestObject() }

	init {
		table.recreate()

		table.insert(LocationTestObject(location1 = Location(world, 0.0, 5.0, 0.0), location2 = Location(null, 0.0, 10.0, 0.0), world = world))

		connection.driver.setSqlLogger(ConsoleSqlLogger)
	}

	@Test
	fun selectAll() {
		val result = table.select().list()

		assertEquals(result.size, 1)

		assertEquals(id, result.first().location1.world.uid)
		assertEquals(id, result.first().locationWorld.uid)
		assertEquals(null, result.first().location2.world)

		assertEquals(id, result.first().world.uid)
	}

	@Test
	fun selectValue() {
		assertEquals(5.0, table.selectValue(property(LocationTestObject::location1).y).first())
		assertEquals(10.0, table.selectValue(property(LocationTestObject::location2).y).first())

		assertEquals(id, table.selectValue(property(LocationTestObject::location1).world).first().uid)
		assertEquals(null, table.selectValue(property(LocationTestObject::location2).world).first())
	}

	@Test
	fun update() {
		table.update(property(LocationTestObject::location1).y to value(20))
		assertEquals(20.0, table.selectValue(property(LocationTestObject::location1).y).first())
	}
}