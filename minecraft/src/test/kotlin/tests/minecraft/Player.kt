package tests.minecraft

import de.mineking.database.*
import de.mineking.database.vendors.postgres.PostgresConnection
import de.mineking.database.vendors.postgres.PostgresMappers
import org.bukkit.OfflinePlayer
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import setup.ConsoleSqlLogger
import setup.createPlayer
import setup.createServer
import setup.recreate
import java.util.*

data class PlayerDao(
	@AutoIncrement @Key @Column val id: Int = 0,
	@Column val player: OfflinePlayer = createPlayer()
)

class PlayerTest {
	val connection = PostgresConnection("localhost:5432/test", user = "test", password = "test")
	val table: Table<PlayerDao>

	val id1 = UUID.randomUUID()
	val id2 = UUID.randomUUID()

	val players = listOf(
		createPlayer(id1),
		createPlayer(id2)
	)

	init {
		connection.registerMinecraftMappers(createServer(players = players), PostgresMappers.STRING, PostgresMappers.UUID_MAPPER, PostgresMappers.ARRAY, PostgresMappers.DOUBLE)
		table = connection.getTable(name = "player_test") { PlayerDao() }

		table.recreate()

		table.insert(PlayerDao(player = players[0]))
		table.insert(PlayerDao(player = players[1]))

		connection.driver.setSqlLogger(ConsoleSqlLogger)
	}

	@Test
	fun selectAll() {
		val result = table.select().list()

		assertEquals(result.size, 2)
		assertEquals(id1, result[0].player.uniqueId)
		assertEquals(id2, result[1].player.uniqueId)
	}

	@Test
	fun selectColumn() {
		assertEquals(id1, table.selectValue(property(PlayerDao::player), limit = 1).first().uniqueId)

		assertEquals(1, table.selectRowCount(where = property(PlayerDao::player) isEqualTo value(id1)))
		assertEquals(1, table.selectRowCount(where = property(PlayerDao::player) isEqualTo value(createPlayer(id1))))
	}
}