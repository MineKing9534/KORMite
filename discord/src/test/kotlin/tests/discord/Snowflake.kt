package tests.discord

import de.mineking.database.*
import net.dv8tion.jda.api.entities.Guild
import net.dv8tion.jda.api.entities.ISnowflake
import net.dv8tion.jda.api.entities.Role
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import setup.*

data class SnowflakeTestObject(
	@AutoIncrement @Key @Column val id: Int = 0,
	@Column val snowflake: ISnowflake? = null,
)

class SnowflakeTest {
	val guilds = listOf(
		createSnowflake<Guild>(1),
		createSnowflake<Guild>(2)
	)

	val roles = listOf(
		createSnowflake<Role>(2)
	)

	//Use STRING mappers because they are more likely to fail
	val connection = createConnection().apply { registerDiscordStringMappers(createJDA(guilds, roles)) }
	val table = connection.getDefaultTable(name = "snowflake_test") { SnowflakeTestObject() }

	init {
		table.recreate()

		table.insert(SnowflakeTestObject(snowflake = guilds[0]))

		connection.driver.setSqlLogger(ConsoleSqlLogger)
	}

	@Test
	fun selectAll() {
		val result = table.select().list()
		assertEquals(1, result.size)
		assertEquals(guilds[0], result[0].snowflake)
	}

	@Test
	fun selectColumn() {
		assertEquals(guilds[0], table.selectValue(property(SnowflakeTestObject::snowflake)).first())
	}

	@Test
	fun selectCondition() {
		assertEquals(1, table.selectRowCount(where = property(SnowflakeTestObject::snowflake).id isEqualTo value(guilds[0])))
		assertEquals(0, table.selectRowCount(where = property(SnowflakeTestObject::snowflake).id isEqualTo value(guilds[1])))
	}

	@Test
	fun update(){
		table.update(property(SnowflakeTestObject::snowflake) to value(roles[0]))
		assertEquals(roles[0], table.select().first().snowflake)
	}
}