package setup

import net.dv8tion.jda.api.JDA
import net.dv8tion.jda.api.entities.*
import net.dv8tion.jda.api.entities.channel.Channel
import net.dv8tion.jda.api.entities.emoji.CustomEmoji
import java.lang.reflect.Proxy

inline fun <reified T> createImplementation(crossinline handler: (String, Array<Any>) -> Any?): T = Proxy.newProxyInstance(T::class.java.classLoader, arrayOf(T::class.java)) { _, method, params -> handler(method.name, params ?: emptyArray()) } as T

inline fun <reified T: ISnowflake> createSnowflake(id: Long = 0) = createImplementation<T> { method, params -> when (method) {
	"getId" -> id.toString()
	"getIdLong" -> id
	"equals" -> {
		val other = params[0] as ISnowflake
		id == other.idLong
	}
	else -> error("Method not supported: $method")
} }

fun createJDA(guilds: List<Guild>, roles: List<Role>, channels: List<Channel>, emojis: List<CustomEmoji>, events: List<ScheduledEvent>, users: List<User>) = createImplementation<JDA> { method, params -> when (method) {
	"getGuildById" -> guilds.find { it.id == params[0].toString() }
	"getRoleById" -> roles.find { it.id == params[0].toString() }
	"getChannelById" -> channels.find { it.id == params[1].toString() }
	"getEmojiById" -> emojis.find { it.id == params[0].toString() }
	"getScheduledEventById" -> events.find { it.id == params[0].toString() }
	"getUserById" -> users.find { it.id == params[0].toString() }
	else -> error("Method not supported: $method")
} }