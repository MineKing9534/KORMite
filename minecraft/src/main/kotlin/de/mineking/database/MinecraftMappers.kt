package de.mineking.database

import io.papermc.paper.command.brigadier.argument.ArgumentTypes.world
import net.kyori.adventure.text.format.NamedTextColor
import net.kyori.adventure.text.format.TextColor
import org.bukkit.Location
import org.bukkit.Server
import org.bukkit.World
import org.jdbi.v3.core.argument.Argument
import java.util.*
import kotlin.reflect.KProperty
import kotlin.reflect.KType
import kotlin.reflect.full.isSubtypeOf
import kotlin.reflect.typeOf

fun DatabaseConnection.registerMinecraftMappers(
	server: Server,
	textType: TypeMapper<String?, *>? = null,
	uuidType: TypeMapper<UUID?, *>? = null,
	arrayType: TypeMapper<*, Array<*>?>? = null,
	doubleType: TypeMapper<Double?, *>? = null
) {
	data["server"] = server

	if (uuidType != null) typeMappers += delegatedTypeMapper(uuidType, { it?.let { server.getOfflinePlayer(it) } }, { it?.uniqueId })
	if (textType != null) typeMappers += delegatedTypeMapper<TextColor?, String?>(textType, { it?.let { TextColor.fromHexString(it) } }, { it?.asHexString() })
	if (textType != null) typeMappers += delegatedTypeMapper(textType, { it?.let { NamedTextColor.NAMES.value(it.lowercase()) } }, { it?.let { NamedTextColor.NAMES.key(it)?.uppercase() } })

	val worldMapper = if (uuidType != null) delegatedTypeMapper(uuidType, { it?.let { server.getWorld(it) } }, { it?.uid }) else null
	if (worldMapper != null) typeMappers += worldMapper

	if (worldMapper != null && doubleType != null && arrayType != null) typeMappers += object : TypeMapper<Location?, Array<String?>?> {
		override fun accepts(manager: DatabaseConnection, property: KProperty<*>?, type: KType): Boolean = type.isSubtypeOf(typeOf<Location?>())
		override fun getType(column: ColumnData<*, *>?, table: TableStructure<*>, type: KType): DataType = arrayType.getType(column, table, typeOf<Array<String?>>())

		override fun format(column: ColumnContext, table: TableStructure<*>, type: KType, value: Location?): Array<String?>? = value?.let {
			arrayOf(it.x.toString(), it.y.toString(), it.z.toString(), it.world?.uid?.toString())
		}
		override fun createArgument(column: ColumnContext, table: TableStructure<*>, type: KType, value: Array<String?>?): Argument = arrayType.createArgument(column, table, typeOf<Array<String?>>(), value)

		@Suppress("UNCHECKED_CAST")
		override fun extract(column: ColumnContext, type: KType, context: ReadContext, position: Int): Array<String?>? = arrayType.extract(column, typeOf<Array<String?>>(), context, position) as Array<String?>?
		override fun parse(column: ColumnContext, type: KType, value: Array<String?>?, context: ReadContext, position: Int): Location? = value?.let {
			val world = value[3]?.let { server.getWorld(UUID.fromString(it)) }
			Location(world, value[0]!!.toDouble(), value[1]!!.toDouble(), value[2]!!.toDouble())
		}
	}
}

@Suppress("UNCHECKED_CAST")
val Node<Location>.x get() = (this + "[1]").castTo<Double>()
@Suppress("UNCHECKED_CAST")
val Node<Location>.y get() = (this + "[2]").castTo<Double>()
@Suppress("UNCHECKED_CAST")
val Node<Location>.z get() = (this + "[3]").castTo<Double>()
@Suppress("UNCHECKED_CAST")
val Node<Location>.world get() = (this + "[4]").castTo<World>()