package de.mineking.database

import net.dv8tion.jda.api.JDA
import net.dv8tion.jda.api.entities.*
import net.dv8tion.jda.api.entities.channel.Channel
import net.dv8tion.jda.api.entities.emoji.CustomEmoji
import org.jdbi.v3.core.argument.Argument
import kotlin.reflect.KProperty
import kotlin.reflect.KType
import kotlin.reflect.full.isSubtypeOf
import kotlin.reflect.typeOf

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
	enumMapper: TypeMapper<Enum<*>?, *>? = null,
	mapper: TypeMapper<T?, *>,
	crossinline converter: (T) -> Long,
	crossinline extractor: (ISnowflake?) -> T?
) {
	data["bot"] = bot

	if (enumMapper != null) {
		typeMappers += object : TypeMapper<ISnowflake, T> {
			override fun accepts(manager: DatabaseConnection, property: KProperty<*>?, type: KType): Boolean = type.isSubtypeOf(typeOf<ISnowflake?>())
			override fun getType(column: ColumnData<*, *>?, table: TableStructure<*>, type: KType): DataType = mapper.getType(column, table, type)

			override fun <O : Any> initialize(column: DirectColumnData<O, *>, type: KType) {
				val name = "${column.baseName}Type"
				column.createVirtualChild(
					name,
					column.table.namingStrategy.getName(name),
					column.table.namingStrategy.getName("Type"),
					enumMapper,
					typeOf<SnowflakeType>()
				) { SnowflakeType.of(column.get(it)) }
			}

			override fun format(column: ColumnData<*, *>?, table: TableStructure<*>, type: KType, value: ISnowflake): T = extractor(value)!!
			override fun createArgument(column: ColumnData<*, *>?, table: TableStructure<*>, type: KType, value: T): Argument = mapper.write(column, table, type, value)

			override fun extract(column: DirectColumnData<*, *>?, type: KType, context: ReadContext, name: String): T = mapper.read(column, type, context, name)!!
			override fun parse(column: DirectColumnData<*, *>?, type: KType, value: T, context: ReadContext, name: String): ISnowflake {
				val typeColumn = column?.getChildren()?.first()
				val snowflakeType = typeColumn?.mapper?.read(column, typeColumn.type, context, typeColumn.name) as SnowflakeType? ?: throw IllegalArgumentException("Cannot create snowflake without type")

				return snowflakeType.getter(bot, converter(value)) as ISnowflake
			}
		}
	}

	typeMappers += typeMapper<Guild?, T?>(mapper, { it?.let { bot.getGuildById(converter(it)) } }, extractor)
	typeMappers += typeMapper<Role?, T?>(mapper, { it?.let { bot.getRoleById(converter(it)) } }, extractor)
	typeMappers += typeMapper<Channel?, T?>(mapper, { it?.let { bot.getChannelById(Channel::class.java, converter(it)) } }, extractor)
	typeMappers += typeMapper<CustomEmoji?, T?>(mapper, { it?.let { bot.getEmojiById(converter(it)) } }, extractor)
	typeMappers += typeMapper<ScheduledEvent?, T?>(mapper, { it?.let { bot.getScheduledEventById(converter(it)) } }, extractor)

	//Snowflake first because mappers registered late will be prioritized
	typeMappers += typeMapper<UserSnowflake?, T?>(mapper, { it?.let { UserSnowflake.fromId(converter(it)) } }, extractor)
	typeMappers += typeMapper<User?, T?>(mapper, { it?.let { bot.getUserById(converter(it)) } }, extractor)
}

fun DatabaseConnection.registerDiscordLongMappers(
	bot: JDA,
	longType: TypeMapper<Long?, *>,
	enumType: TypeMapper<Enum<*>?, *>? = null
) = registerDiscordMappers(bot, enumType, longType, { it }) { it?.idLong }

fun DatabaseConnection.registerDiscordStringMappers(
	bot: JDA,
	stringType: TypeMapper<String?, *>,
	enumType: TypeMapper<Enum<*>?, *>? = null
) = registerDiscordMappers(bot, enumType, stringType, { it.toLong() }) { it?.id }