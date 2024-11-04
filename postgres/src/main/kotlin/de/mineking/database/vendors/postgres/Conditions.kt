package de.mineking.database.vendors.postgres

import de.mineking.database.Node
import de.mineking.database.Where
import de.mineking.database.invoke
import de.mineking.database.value
import kotlin.reflect.typeOf

@Suppress("UNCHECKED_CAST")
operator fun <T, C> Node<C>.get(index: Node<Int>): Node<T> where C: Iterable<T> = (this + "[" + index + " + 1]") as Node<T>

@Suppress("UNCHECKED_CAST")
operator fun <T, C> Node<C>.get(index: Int): Node<T> where C: Iterable<T> = (this + "[" + value(index + 1) + "]") as Node<T>

@Suppress("UNCHECKED_CAST")
val <C> Node<C>.size where C: Iterable<*> get() = "array_length"(this, value(1)) as Node<Int>

infix fun <T, C> Node<C>.contains(other: Node<T>) where C: Iterable<T> = Where(other + " = any(" + this + ")")

infix fun Node<CharSequence>.matches(other: String) = Where(this + " ~ '" + other + "'")

@Suppress("UNCHECKED_CAST")
fun <T> Node<*>.json(vararg path: String) = (this + " #>> " + value(path, typeOf<Array<String>>(), static = true)) as Node<T>