package de.mineking.database

import com.google.gson.JsonElement
import com.google.gson.JsonPrimitive
import de.mineking.database.vendors.postgres.PostgresConnection
import de.mineking.database.vendors.postgres.json
import de.mineking.database.vendors.postgres.jsonTypeMapper
import net.dv8tion.jda.api.JDA
import net.dv8tion.jda.api.entities.*
import net.dv8tion.jda.api.entities.channel.Channel
import net.dv8tion.jda.api.entities.emoji.CustomEmoji

enum class SnowflakeType(val getter: (JDA, Long) -> ISnowflake?) {
	USER(JDA::getUserById),
	GUILD(JDA::getGuildById),
	ROLE(JDA::getRoleById);

	companion object {
		fun of(obj: Any?) = when (obj) {
			is User -> USER
			is Guild -> GUILD
			is Role -> ROLE
			else -> throw IllegalArgumentException()
		}
	}
}

internal inline fun <reified T> DatabaseConnection.registerDiscordMappers(
	bot: JDA,
	mapper: TypeMapper<T?, *>,
	crossinline converter: (T) -> Long,
	crossinline extractor: (ISnowflake) -> T
) {
	data["bot"] = bot
	
	typeMappers += nullsafeDelegateTypeMapper<Guild, T>(mapper, { it, _ -> bot.getGuildById(converter(it)) }, extractor)
	typeMappers += nullsafeDelegateTypeMapper<Role, T>(mapper, { it, _ -> bot.getRoleById(converter(it)) }, extractor)
	typeMappers += nullsafeDelegateTypeMapper<Channel, T>(mapper, { it, _ -> bot.getChannelById(Channel::class.java, converter(it)) }, extractor)
	typeMappers += nullsafeDelegateTypeMapper<CustomEmoji, T>(mapper, { it, _ -> bot.getEmojiById(converter(it)) }, extractor)
	typeMappers += nullsafeDelegateTypeMapper<ScheduledEvent, T>(mapper, { it, _ -> bot.getScheduledEventById(converter(it)) }, extractor)

	//Snowflake first because mappers registered late will be prioritized
	typeMappers += nullsafeDelegateTypeMapper<UserSnowflake, T>(mapper, { it, type -> UserSnowflake.fromId(converter(it)) }, extractor)
	typeMappers += nullsafeDelegateTypeMapper<User, T>(mapper, { it, _ -> bot.getUserById(converter(it)) }, extractor)
}

fun DatabaseConnection.registerDiscordLongMappers(bot: JDA, longType: TypeMapper<Long?, *> = getTypeMapper<Long?>()) = registerDiscordMappers(bot, longType, { it }) { it.idLong }
fun DatabaseConnection.registerDiscordStringMappers(bot: JDA, stringType: TypeMapper<String?, *> = getTypeMapper<String?>()) = registerDiscordMappers(bot, stringType, { it.toLong() }) { it.id }

/**
 * Has to be registered before the normal mappers
 */
fun PostgresConnection.registerSnowflakeMapper(
	bot: JDA,
	extractor: (ISnowflake) -> JsonElement
) {
	typeMappers += jsonTypeMapper<ISnowflake>(
		{
			val type = SnowflakeType.valueOf(get("type").asString)
			type.getter(bot, get("id").asLong)
		},
		{
			addProperty("type", SnowflakeType.of(it).name)
			add("id", extractor(it))
		}
	)
}

fun PostgresConnection.registerDiscordLongMappers(bot: JDA) {
	registerSnowflakeMapper(bot) { JsonPrimitive(it.idLong) }
	(this as DatabaseConnection).registerDiscordLongMappers(bot)
}

fun PostgresConnection.registerDiscordStringMappers(bot: JDA) {
	registerSnowflakeMapper(bot) { JsonPrimitive(it.id) }
	(this as DatabaseConnection).registerDiscordStringMappers(bot)
}

val Node<ISnowflake?>.id get() = this.json<Long>("id")
val Node<ISnowflake?>.type get() = this.json<SnowflakeType>("type")