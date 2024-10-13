package de.mineking.database

import java.util.*
import kotlin.reflect.KType
import kotlin.reflect.full.isSubtypeOf
import kotlin.reflect.jvm.jvmErasure
import kotlin.reflect.typeOf

fun KType.isArray(): Boolean = jvmErasure.java.isArray || isSubtypeOf(typeOf<Collection<*>>())
fun KType.component(): KType = arguments[0].type ?: typeOf<Any>()
fun KType.componentIfArray(): KType = if (isArray()) component() else this

fun KType.createCollection(content: Array<*>): Any = when {
	jvmErasure.java.isArray -> content
	isSubtypeOf(typeOf<List<*>>()) -> arrayListOf(*content)
	isSubtypeOf(typeOf<EnumSet<*>>()) -> {
		@Suppress("UNCHECKED_CAST")
		fun <E: Enum<E>> createEnumSet() = if (content.isEmpty()) EnumSet.noneOf(jvmErasure.java as Class<E>) else EnumSet.copyOf(listOf(*content) as List<E>)
		createEnumSet()
	}
	isSubtypeOf(typeOf<Set<*>>()) -> hashSetOf(*content)
	else -> throw IllegalArgumentException("Cannot create collection of type $this")
}