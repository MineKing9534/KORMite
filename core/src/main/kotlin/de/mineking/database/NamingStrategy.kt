package de.mineking.database

fun interface NamingStrategy {
	fun getName(original: String): String

	companion object {
		val KEEP = NamingStrategy { it }
		val LOWERCASE = NamingStrategy { it.lowercase() }
		val SNAKE_CASE = NamingStrategy { it.replace("(?<=[^[A-Z]])[A-Z]".toRegex(), "_$0").lowercase() }

		val DEFAULT = SNAKE_CASE
	}
}