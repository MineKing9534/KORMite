package de.mineking.database

import de.mineking.database.vendors.postgres.PostgresConnection
import de.mineking.database.vendors.postgres.json
import de.mineking.database.vendors.postgres.jsonTypeMapper
import net.kyori.adventure.text.format.NamedTextColor
import net.kyori.adventure.text.format.TextColor
import org.bukkit.Location
import org.bukkit.OfflinePlayer
import org.bukkit.Server
import org.bukkit.World
import java.util.*

fun DatabaseConnection.registerMinecraftMappers(
	server: Server,
	textType: TypeMapper<String?, *> = getTypeMapper<String?>(),
	uuidType: TypeMapper<UUID?, *> = getTypeMapper<UUID?>()
) {
	data["server"] = server

	typeMappers += nullsafeDelegateTypeMapper<OfflinePlayer, UUID>(uuidType, { it, _ -> server.getOfflinePlayer(it) }, OfflinePlayer::getUniqueId)
	typeMappers += nullsafeDelegateTypeMapper<World, UUID>(uuidType, { it, _ -> server.getWorld(it) }, World::getUID)

	typeMappers += nullsafeDelegateTypeMapper<TextColor, String>(textType, { it, _ -> TextColor.fromHexString(it) }, TextColor::asHexString)
	typeMappers += nullsafeDelegateTypeMapper<NamedTextColor, String>(textType, { it, _ -> NamedTextColor.NAMES.value(it.lowercase()) }, { NamedTextColor.NAMES.key(it)?.uppercase() })
}

fun PostgresConnection.registerMinecraftMappers(server: Server) {
	(this as DatabaseConnection).registerMinecraftMappers(server)
	typeMappers += jsonTypeMapper<Location>(
		{
			val world = if (get("world").isJsonNull) null else server.getWorld(UUID.fromString(get("world").asString))
			Location(
				world,
				get("x").asDouble,
				get("y").asDouble,
				get("z").asDouble,
			)
		}, {
			addProperty("x", it.x)
			addProperty("y", it.y)
			addProperty("z", it.z)
			addProperty("world", it.world?.uid?.toString())
		}
	)
}

val Node<Location>.x get() = this.json<Double>("x")
val Node<Location>.y get() = this.json<Double>("y")
val Node<Location>.z get() = this.json<Double>("z")
val Node<Location>.world get() = this.json<World>("world")