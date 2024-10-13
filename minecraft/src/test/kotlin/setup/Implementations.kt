package setup

import org.bukkit.OfflinePlayer
import org.bukkit.Server
import org.bukkit.World
import java.lang.reflect.Proxy
import java.util.*

val ZERO_UID = UUID(0, 0)

inline fun <reified T> createImplementation(crossinline handler: (String, Array<Any>) -> Any?): T = Proxy.newProxyInstance(T::class.java.classLoader, arrayOf(T::class.java)) { _, method, params -> handler(method.name, params ?: emptyArray()) } as T

fun createServer(worlds: List<World> = emptyList(), players: List<OfflinePlayer> = emptyList()) = createImplementation<Server> { method, params -> when(method) {
	"getWorld" -> worlds.find { it.uid == params[0] }
	"getOfflinePlayer" -> players.find { it.uniqueId == params[0] }
	else -> error("Method not supported: $method")
} }

fun createWorld(uuid: UUID = ZERO_UID) = createImplementation<World> { method, _ -> when(method) {
	"getUID" -> uuid
	else -> error("Method not supported: $method")
} }

fun createPlayer(uuid: UUID = ZERO_UID) = createImplementation<OfflinePlayer> { method, _ -> when(method) {
	"getUniqueId" -> uuid
	else -> error("Method not supported: $method")
} }