package de.mineking.database.vendors.postgres

import de.mineking.database.Node
import de.mineking.database.Where
import de.mineking.database.invoke
import de.mineking.database.value
import kotlin.reflect.typeOf

fun arrayLength(node: Node) = "array_length"(node)

infix fun Node.matches(other: String) = Where(this + " ~ '" + other + "'")
infix fun Node.contains(other: Node) = Where(other + " = any(" + this + ")")

fun Node.json(vararg path: String) = this + " #>> " + value(path, typeOf<Array<String>>(), static = true)