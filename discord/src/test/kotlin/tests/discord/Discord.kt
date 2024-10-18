package tests.discord

import de.mineking.database.*
import de.mineking.database.vendors.PostgresConnection
import de.mineking.database.vendors.PostgresMappers
import net.dv8tion.jda.api.entities.*
import net.dv8tion.jda.api.entities.channel.middleman.MessageChannel
import net.dv8tion.jda.api.entities.emoji.CustomEmoji
import net.dv8tion.jda.api.entities.emoji.RichCustomEmoji
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import setup.ConsoleSqlLogger
import setup.createJDA
import setup.createSnowflake
import setup.recreate

data class DiscordDao(
	@AutoIncrement @Key @Column val id: Int = 0,
	@Column val guild: Guild = createSnowflake(),
	@Column val role: Role = createSnowflake(),
	@Column val channel: MessageChannel = createSnowflake(),
	@Column val emoji: CustomEmoji = createSnowflake(),
	@Column val event: ScheduledEvent = createSnowflake(),
	@Column val userSnowflake: UserSnowflake = createSnowflake(),
	@Column val user: User = createSnowflake()
)

class DiscordTest {
	val connection = PostgresConnection("localhost:5432/test", user = "test", password = "test")
	val table: Table<DiscordDao>

	val guilds = listOf(
		createSnowflake<Guild>(1),
		createSnowflake<Guild>(2)
	)

	val roles = listOf(
		createSnowflake<Role>(3),
		createSnowflake<Role>(4)
	)

	val channels = listOf(
		createSnowflake<MessageChannel>(5),
		createSnowflake<MessageChannel>(6)
	)

	val emojis = listOf(
		createSnowflake<RichCustomEmoji>(7),
		createSnowflake<RichCustomEmoji>(8)
	)

	val events = listOf(
		createSnowflake<ScheduledEvent>(9),
		createSnowflake<ScheduledEvent>(10)
	)

	val users = listOf(
		createSnowflake<User>(11),
		createSnowflake<User>(12)
	)

	val userSnowflakes = listOf(
		createSnowflake<UserSnowflake>(13),
		createSnowflake<UserSnowflake>(14)
	)

	init {
		//Use STRING mappers because they are more likely to fail
		connection.registerDiscordStringMappers(createJDA(guilds, roles, channels, emojis, events, users), PostgresMappers.STRING)
		table = connection.getTable(name = "discord_test") { DiscordDao() }

		table.recreate()

		table.insert(DiscordDao(guild = guilds[0], role = roles[0], channel = channels[0], emoji = emojis[0], event = events[0], user = users[0], userSnowflake = userSnowflakes[0]))

		connection.driver.setSqlLogger(ConsoleSqlLogger)
	}

	@Test
	fun selectAll() {
		val result = table.select().list()
		assertEquals(1, result.size)
		assertEquals(guilds[0], result[0].guild)
	}

	@Test
	fun selectColumn() {
		assertEquals(guilds[0], table.select<Guild>(property("guild")).first())
	}

	@Test
	fun selectCondition() {
		assertEquals(1, table.selectRowCount(where = property("guild") isEqualTo value(guilds[0])))
		assertEquals(0, table.selectRowCount(where = property("guild") isEqualTo value(guilds[1])))
	}
}