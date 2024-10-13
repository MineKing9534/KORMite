package de.mineking.database

import net.dv8tion.jda.api.JDA
import net.dv8tion.jda.api.entities.*
import net.dv8tion.jda.api.entities.channel.Channel
import net.dv8tion.jda.api.entities.emoji.CustomEmoji

internal inline fun <reified T> DatabaseConnection.registerDiscordMappers(bot: JDA, type: TypeMapper<T?, *>, crossinline converter: (T) -> Long, crossinline extractor: (ISnowflake?) -> T?) {
	data["bot"] = bot

	typeMappers += typeMapper<Guild?, T?>(type, { it?.let { bot.getGuildById(converter(it)) } }, extractor)
	typeMappers += typeMapper<Role?, T?>(type, { it?.let { bot.getRoleById(converter(it)) } }, extractor)
	typeMappers += typeMapper<Channel?, T?>(type, { it?.let { bot.getChannelById(Channel::class.java, converter(it)) } }, extractor)
	typeMappers += typeMapper<CustomEmoji?, T?>(type, { it?.let { bot.getEmojiById(converter(it)) } }, extractor)
	typeMappers += typeMapper<ScheduledEvent?, T?>(type, { it?.let { bot.getScheduledEventById(converter(it)) } }, extractor)

	//Snowflake first because mappers registered late will be prioritized
	typeMappers += typeMapper<UserSnowflake?, T?>(type, { it?.let { UserSnowflake.fromId(converter(it)) } }, extractor)
	typeMappers += typeMapper<User?, T?>(type, { it?.let { bot.getUserById(converter(it)) } }, extractor)
}

fun DatabaseConnection.registerDiscordLongMappers(bot: JDA, longType: TypeMapper<Long?, *>) = registerDiscordMappers(bot, longType, { it }) { it?.idLong }
fun DatabaseConnection.registerDiscordStringMappers(bot: JDA, stringType: TypeMapper<String?, *>) = registerDiscordMappers(bot, stringType, { it.toLong() }) { it?.id }