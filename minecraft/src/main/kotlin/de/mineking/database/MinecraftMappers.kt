package de.mineking.database

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

@Target(AnnotationTarget.FIELD)
@Retention(AnnotationRetention.RUNTIME)
annotation class LocationWorldColumn(val name: String)

fun DatabaseConnection.registerMinecraftMappers(
	server: Server,
	textType: TypeMapper<String?, *>? = null,
	uuidType: TypeMapper<UUID?, *>? = null,
	arrayType: TypeMapper<*, Array<*>?>? = null,
	doubleType: TypeMapper<Double?, *>? = null
) {
	data["server"] = server

	if (uuidType != null) typeMappers += typeMapper(uuidType, { it?.let { server.getOfflinePlayer(it) } }, { it?.uniqueId })
	if (textType != null) typeMappers += typeMapper<TextColor?, String?>(textType, { it?.let { TextColor.fromHexString(it) } }, { it?.asHexString() })
	if (textType != null) typeMappers += typeMapper(textType, { it?.let { NamedTextColor.NAMES.value(it.lowercase()) } }, { it?.let { NamedTextColor.NAMES.key(it)?.uppercase() } })

	val worldMapper = if (uuidType != null) typeMapper(uuidType, { it?.let { server.getWorld(it) } }, { it?.uid }) else null
	if (worldMapper != null) typeMappers += worldMapper

	if (worldMapper != null && doubleType != null && arrayType != null) typeMappers += object : TypeMapper<Location?, Array<Double>?> {
		override fun accepts(manager: DatabaseConnection, property: KProperty<*>?, type: KType): Boolean = type.isSubtypeOf(typeOf<Location?>())
		override fun getType(column: ColumnData<*, *>?, table: TableStructure<*>, type: KType): DataType = arrayType.getType(column, table, typeOf<Array<Double>>())

		override fun <O: Any> initialize(column: DirectColumnData<O, *>, type: KType) {
			val worldColumn = column.property.getDatabaseAnnotation<LocationWorldColumn>()

			if (worldColumn == null) {
				//We need to check the column type here directly to ensure column.get returns a Location instance
				require(column.type.isSubtypeOf(typeOf<Location?>())) { "Please specify a column to use to store the world in when not using a direct Location column" }

				val name = "${ column.baseName }World"
				column.createVirtualChild(
					name,
					column.table.namingStrategy.getName(name),
					column.table.namingStrategy.getName("World"),
					worldMapper,
					typeOf<World>()
				) { (column.get(it) as Location?)?.world }
			} else {
				@Suppress("UNCHECKED_CAST")
				val world = parseColumnSpecification(worldColumn.name, column.table).column as ColumnData<O, World>

				column.createVirtualChild(
					world.baseName,
					world.name,
					column.table.namingStrategy.getName("World"),
					world.mapper,
					world.type,
					isReference = true
				) { world.get(it) }
			}

			column.createVirtualChild("", column.name, "x", doubleType, typeOf<Double>(), isReference = true, transform = { "\"$it\"[1]" }) { (column.get(it) as Location?)?.x }
			column.createVirtualChild("", column.name, "y", doubleType, typeOf<Double>(), isReference = true, transform = { "\"$it\"[2]" }) { (column.get(it) as Location?)?.y }
			column.createVirtualChild("", column.name, "z", doubleType, typeOf<Double>(), isReference = true, transform = { "\"$it\"[3]" }) { (column.get(it) as Location?)?.z }
		}

		override fun format(column: ColumnData<*, *>?, table: TableStructure<*>, type: KType, value: Location?): Array<Double>? = value?.let { arrayOf(it.x, it.y, it.z) }
		override fun createArgument(column: ColumnData<*, *>?, table: TableStructure<*>, type: KType, value: Array<Double>?): Argument = arrayType.createArgument(column, table, typeOf<Array<Double>>(), value)

		@Suppress("UNCHECKED_CAST")
		override fun extract(column: DirectColumnData<*, *>?, type: KType, context: ReadContext, name: String): Array<Double>? = arrayType.extract(column, typeOf<Array<Double>>(), context, name) as Array<Double>?
		override fun parse(column: DirectColumnData<*, *>?, type: KType, value: Array<Double>?, context: ReadContext, name: String): Location? = value?.let {
			val worldColumn = column?.getChildren()?.first()
			val world = worldColumn?.mapper?.read(column, worldColumn.type, context, worldColumn.name) as World?

			Location(world, value[0], value[1], value[2])
		}
	}
}