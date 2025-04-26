package de.mineking.database

import org.jdbi.v3.core.argument.Argument
import kotlin.reflect.KProperty
import kotlin.reflect.KProperty1
import kotlin.reflect.KType
import kotlin.reflect.jvm.javaField
import kotlin.reflect.typeOf

fun Collection<Node<*>>.join(delimiter: Node<*> = unsafe(", ")): Node<*> {
	var result = Node.EMPTY

	val iterator = iterator()
	while (iterator.hasNext()) {
		val node = iterator.next()
		result += node

		if (iterator.hasNext()) result += delimiter
	}

	return result
}

operator fun String.invoke(vararg params: Node<*>) = Node.EMPTY + this + "(" + params.toList().join() + ")"

@Suppress("UNCHECKED_CAST")
val Node<String>.length get() = "length"(this) as Node<Int>

@Suppress("UNCHECKED_CAST")
fun Node<String>.lowercase() = "lower"(this) as Node<String>

@Suppress("UNCHECKED_CAST")
fun Node<String>.uppercase() = "upper"(this) as Node<String>

@Suppress("UNCHECKED_CAST")
infix fun Node<String>.concat(other: Node<String>) = "concat"(this, other) as Node<String>

data class Case<T>(val condition: Where, val value: Node<T>)
infix fun <T> Where.then(value: Node<T>): Case<T> = Case(this, value)

fun <T> case(vararg cases: Case<T>, default: Node<*>? = null): Node<T> {
	val caseNodes = cases.map { (where, node) -> Node.EMPTY + "when " + where + " then " + node }.toMutableList()
	if (default != null) caseNodes += default.let { unsafe("else ") + it }

	@Suppress("UNCHECKED_CAST")
	return (Node.EMPTY + "(case " + caseNodes.join(unsafe(" ")) + " end)") as Node<T>
}

@Suppress("UNCHECKED_CAST")
infix fun <T> Node<out T?>.orDefault(other: Node<T>) = "coalesce"(this, other) as Node<T>

@Suppress("UNCHECKED_CAST")
fun <T> Node<*>.castTo(type: DataType) = "cast"(this + " as ${type.sqlName}") as Node<T>

inline fun <reified T> Node<*>.castTo() = object : Node<T> {
	override fun format(table: TableStructure<*>, prefix: Boolean): String {
		val original = this@castTo.format(table, prefix)
		if (!prefix) return original //dont cast in update context

		val column = columnContext(table).lastOrNull()
		val mapper = table.manager.getTypeMapper<T, Any?>(typeOf<T>(), column?.property)

		return "cast($original as ${ mapper.getType(column, table, typeOf<T>()).sqlName })"
	}

	override fun values(table: TableStructure<*>, column: ColumnContext) = this@castTo.values(table, column)
	override fun columnContext(table: TableStructure<*>) = this@castTo.columnContext(table)
	override fun columns(table: TableStructure<*>) = this@castTo.columns(table)
}

fun createContext(properties: List<KProperty<*>>, table: TableStructure<*>, override: Boolean = false): ColumnContext {
	if (properties.isEmpty()) return emptyList()

	val result = ArrayList<PropertyData<*, *>>(properties.size)
	val iterator = properties.iterator()

	var table = table as TableStructure<*>?

	for ((index, current) in iterator.withIndex()) {
		if (table == null) error("Column ${ result.last().name } does not have a reference")

		if (index == 0 && !override) {
			val currentContext = current.javaField?.declaringClass?.kotlin
			if (currentContext != table.component) {
				table = table.manager.findTable(currentContext!!)?.structure ?: error("Could not determine table for ${ current.name }")
			}
		}

		val column = table.properties.firstOrNull { it.property == current } ?: error("Column ${ current.name } not found in ${ table.name }")
		result += column

		table = column.reference?.structure
	}

	return result
}

fun <T> property(properties: List<KProperty<*>>, tableOverride: TableStructure<*>? = null) = PropertyNode<T> { table ->
	if (properties.isEmpty()) error("Need at least one property reference")
	createContext(properties, tableOverride ?: table, tableOverride != null)
}

fun <T> property(property: KProperty<*>, vararg reference: KProperty<*>, tableOverride: TableStructure<*>? = null) =
	property<T>(listOf(property) + reference, tableOverride)

fun <T> property(property: KProperty<T>, tableOverride: TableStructure<*>? = null) =
	property<T>(property, *emptyArray(), tableOverride = tableOverride)

fun <T, I> property(property: KProperty<I>, reference: KProperty1<I, T>, tableOverride: TableStructure<*>? = null) =
	property<T>(property, reference, tableOverride = tableOverride)

fun <T, I1, I2> property(property: KProperty<I1>, reference1: KProperty1<I1, I2>, reference2: KProperty1<I2, T>, tableOverride: TableStructure<*>? = null) =
	property<T>(property, reference1, reference2, tableOverride = tableOverride)

fun <T> property(name: String, tableOverride: TableStructure<*>? = null) = PropertyNode<T> { table ->
	val column = (tableOverride ?: table).getFromCode(name) ?: error("Column $name not found in ${ table.name }")
	listOf(column)
}

//Static values will not use the column context of the current node
fun <T> value(value: T, type: KType, static: Boolean = false): ValueNode<T> = ValueNode<T> { table, context ->
	val column = context.lastOrNull()

	@Suppress("UNCHECKED_CAST")
	val mapper = column?.mapper?.takeIf { !static && it.accepts(table.manager, column.property, type) } as TypeMapper<T, Any?>?
		?: table.manager.getTypeMapper<T, Any?>(type, column?.property?.takeIf { !static })

	mapper.write(context, table, type, value)
}

inline fun <reified T> value(value: T, static: Boolean = false) = value(value, typeOf<T>(), static)

@Suppress("UNCHECKED_CAST")
fun <T> nullValue() = unsafe("null") as Node<T?>

fun unsafe(string: String, values: Map<String, Argument> = emptyMap()) = node<Any?>(string, values)
fun <T> node(string: String, values: Map<String, Argument> = emptyMap()) = object : Node<T> {
	override fun format(table: TableStructure<*>, prefix: Boolean): String = string
	override fun values(table: TableStructure<*>, column: ColumnContext): Map<String, Argument> = values
}

internal const val VARIABLE_TABLE_NAME = "__variables__"

@Suppress("UNCHECKED_CAST")
fun <T> variable(name: String) = VariableNode<T> { name }

//Ensure type safety for update
@Suppress("UNCHECKED_CAST")
infix fun <T> Node<T>.to(other: Node<T>) = ((this as Any) to other) as Pair<Node<T>, Node<T>>

fun <T> Node<T>.withContext(context: (TableStructure<*>) -> ColumnContext, force: Boolean = true) = object : Node<T> by this {
	override fun columnContext(table: TableStructure<*>) = if (force) context(table) else this@withContext.columnContext(table).takeIf { it.isNotEmpty() } ?: context(table)
}

fun <T> Node<T>.withContext(property: String, force: Boolean = true) = withContext({ listOf(it.getFromCode(property) ?: error("Column $property not found in ${ it.name }")) }, force)
fun <T> Node<T>.withContext(properties: List<KProperty<*>>, force: Boolean = true) = withContext({ createContext(properties, it) }, force)
fun <T> Node<T>.withContext(property: KProperty<*>, vararg reference: KProperty<*>, force: Boolean = true) = withContext({ createContext(listOf(property) + reference, it) }, force)

interface Node<T> {
	companion object {
		val EMPTY = unsafe("")
	}

	fun format(table: TableStructure<*>, prefix: Boolean = true): String
	fun values(table: TableStructure<*>, column: ColumnContext = emptyList()): Map<String, Argument> = emptyMap()

	fun columnContext(table: TableStructure<*>): ColumnContext = emptyList()
	fun columns(table: TableStructure<*>): List<ColumnContext> = emptyList()

	fun buildUpdate(table: TableStructure<*>, other: Node<*>): Node<*> = object : Node<Any?> by this + other {
		override fun format(table: TableStructure<*>, prefix: Boolean) = this@Node.format(table, prefix = false) + " = " + other.format(table)
	}

	@Suppress("UNCHECKED_CAST")
	operator fun plus(other: String): Node<T> = (this + unsafe(other)) as Node<T>
	operator fun plus(other: Node<*>): Node<Any?> = object : Node<Any?> {
		override fun format(table: TableStructure<*>, prefix: Boolean) = this@Node.format(table, prefix) + other.format(table, prefix)
		override fun values(table: TableStructure<*>, column: ColumnContext): Map<String, Argument> = this@Node.values(table, column) + other.values(table, column)

		override fun columnContext(table: TableStructure<*>): ColumnContext = this@Node.columnContext(table).takeIf { it.isNotEmpty() } ?: other.columnContext(table)
		override fun columns(table: TableStructure<*>): List<ColumnContext> = this@Node.columns(table) + other.columns(table)
	}
}

fun interface VariableNode<T> : Node<T> {
	fun name(): String
	override fun format(table: TableStructure<*>, prefix: Boolean) = "\"$VARIABLE_TABLE_NAME\".\"${name()}\""
}

fun interface ValueNode<T> : Node<T> {
	val id get() = hashCode().toString()

	fun value(table: TableStructure<*>, column: ColumnContext): Argument

	override fun format(table: TableStructure<*>, prefix: Boolean) = ":$id"
	override fun values(table: TableStructure<*>, column: ColumnContext) = mapOf(id to value(table, column))
}

fun interface PropertyNode<T> : Node<T> {
	override fun format(table: TableStructure<*>, prefix: Boolean): String {
		val context = columnContext(table)
		val column = context.last()

		return column.format(context, prefix)
	}

	override fun columnContext(table: TableStructure<*>): ColumnContext
	override fun columns(table: TableStructure<*>): List<ColumnContext> = listOf(columnContext(table))
}