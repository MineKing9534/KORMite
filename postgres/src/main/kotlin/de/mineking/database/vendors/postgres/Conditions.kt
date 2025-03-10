package de.mineking.database.vendors.postgres

import de.mineking.database.*

@Suppress("UNCHECKED_CAST")
operator fun <T, C: Iterable<T>> Node<C>.get(index: Node<Int>): Node<T> = (this + "[" + index + " + 1]") as Node<T>

@Suppress("UNCHECKED_CAST")
operator fun <T, C: Iterable<T>> Node<C>.get(index: Int): Node<T> = this[value(index)]

@Suppress("UNCHECKED_CAST")
val <C> Node<C>.length where C: Iterable<*> get() = "array_length"(this, unsafeNode("1")) as Node<Int>

infix fun <T, C: Iterable<T>> Node<C>.contains(other: Node<T>) = Where(other + " = any(" + this + ")")

infix fun Node<String>.matches(pattern: Node<String>) = Where(this + " ~ " + pattern)
infix fun Node<String>.matches(pattern: String) = Where(this + " ~ '" + pattern + "'")

infix fun Node<String>.matchesIgnoreCase(pattern: Node<String>) = Where(this + " ~* " + pattern)
infix fun Node<String>.matchesIgnoreCase(pattern: String) = Where(this + " ~* '" + pattern + "'")

fun Node<*>.toJson() = "to_jsonb"(this)

@Suppress("UNCHECKED_CAST")
fun <T> Node<*>.json(vararg path: String): Node<T> {
    val path = value<Array<out String>>(path, static = true)
    return object : Node<T> by (this + " #>> " + path) as Node<T> {
        override fun buildUpdate(table: TableStructure<*>, other: Node<*>) = this@json.buildUpdate(table, "jsonb_set"(this@json.toJson(), path, other.toJson()))
    }
}