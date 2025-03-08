package de.mineking.database

import org.jdbi.v3.core.argument.Argument
import kotlin.reflect.KProperty
import kotlin.reflect.KProperty1
import kotlin.reflect.KType
import kotlin.reflect.full.isSubtypeOf
import kotlin.reflect.typeOf

fun Collection<Node<*>>.join(delimiter: Node<*> = unsafeNode(", ")): Node<*> {
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
	if (default != null) caseNodes += default.let { unsafeNode("else ") + it }

	@Suppress("UNCHECKED_CAST")
	return (Node.EMPTY + "(case " + caseNodes.join(unsafeNode(" ")) + " end)") as Node<T>
}

@Suppress("UNCHECKED_CAST")
infix fun <T> Node<out T?>.orDefault(other: Node<T>) = "coalesce"(this, other) as Node<T>

@Suppress("UNCHECKED_CAST")
fun <T> Node<*>.castTo(type: DataType) = "cast"(this + " as ${type.sqlName}") as Node<T>

inline fun <reified T> Node<*>.castTo() = object : Node<T> by this {
	override fun format(table: TableStructure<*>, prefix: Boolean): String {
		val original = this@castTo.format(table, prefix)

		val column = columnContext(table).lastOrNull()
		val mapper = table.manager.getTypeMapper<T, Any?>(typeOf<T>(), column?.property)

		return "cast($original as ${ mapper.getType(column, table, typeOf<T>()).sqlName })"
	}
}

fun <T> property(properties: List<KProperty<*>>) = PropertyNode<T> { table ->
	if (properties.isEmpty()) error("Need at least one property reference")

	val result = ArrayList<ColumnData<*, *>>(properties.size)
	val iterator = properties.iterator()

	var table = table as TableStructure<*>?

	for (current in iterator) {
		if (table == null) error("Column ${ result.last().name } does not have a reference")

		val column = table.columns.firstOrNull { it.property == current } ?: error("Column ${ current.name } not found in ${ table.name }")
		result += column

		table = column.reference?.structure
	}

	result
}

fun <T> property(property: KProperty<*>, vararg reference: KProperty<*>) = property<T>(listOf(property) + reference)
fun <T> property(property: KProperty<T>) = property<T>(property, *emptyArray())
fun <T, I> property(property: KProperty<I>, reference: KProperty1<I, T>) = property<T>(property, reference)
fun <T, I1, I2> property(property: KProperty<I1>, reference1: KProperty1<I1, I2>, reference2: KProperty1<I2, T>) = property<T>(property, reference1, reference2)

fun <T> property(name: String) = PropertyNode<T> { table ->
	val column = table.getColumnFromCode(name) ?: error("Column $name not found in ${ table.name }")
	listOf(column)
}

//Static values will not use the column context of the current node
fun <T> value(value: T, type: KType, static: Boolean = false): ValueNode<T> = ValueNode<T> { table, context ->
	val column = context.lastOrNull()

	@Suppress("UNCHECKED_CAST")
	val mapper = column?.takeIf { !static && it.type.isSubtypeOf(type) }?.mapper as TypeMapper<T, Any?>? ?: table.manager.getTypeMapper<T, Any?>(type, column?.property?.takeIf { !static })

	mapper.write(context, table, type, value)
}

inline fun <reified T> value(value: T, static: Boolean = false) = value(value, typeOf<T>(), static)

@Suppress("UNCHECKED_CAST")
fun <T> nullValue() = unsafeNode("null") as Node<T?>

fun unsafeNode(string: String, values: Map<String, Argument> = emptyMap()) = object : Node<Any?> {
	override fun format(table: TableStructure<*>, prefix: Boolean): String = string
	override fun values(table: TableStructure<*>, column: ColumnContext): Map<String, Argument> = values
}

//Ensure type safety for update
@Suppress("UNCHECKED_CAST")
infix fun <T> Node<T>.to(other: Node<T>) = ((this as Any) to other) as Pair<Node<T>, Node<T>>

fun <T> Node<T>.withContext(context: (TableStructure<*>) -> ColumnContext, force: Boolean = true) = object : Node<T> by this {
	override fun columnContext(table: TableStructure<*>) = if (force) context(table) else this@withContext.columnContext(table).takeIf { it.isNotEmpty() } ?: context(table)
}

fun <T> Node<T>.withContext(property: String, force: Boolean = true) = withContext({ listOf(it.getColumnFromCode(property) ?: error("Column $property not found in ${ it.name }")) }, force)
fun <T> Node<T>.withContext(property: KProperty<*>, force: Boolean = true) = withContext({ listOf(it.columns.firstOrNull { it.property == property } ?: error("Column $property not found in ${ it.name }")) }, force)

interface Node<T> {
	companion object {
		val EMPTY = unsafeNode("")
	}

	fun format(table: TableStructure<*>, prefix: Boolean = true): String
	fun values(table: TableStructure<*>, column: ColumnContext): Map<String, Argument> = emptyMap()

	fun columnContext(table: TableStructure<*>): ColumnContext = emptyList()
	fun columns(table: TableStructure<*>): List<ColumnContext> = emptyList()

	@Suppress("UNCHECKED_CAST")
	operator fun plus(string: String): Node<T> = (this + unsafeNode(string)) as Node<T>
	operator fun plus(node: Node<*>): Node<Any?> = object : Node<Any?> {
		override fun format(table: TableStructure<*>, prefix: Boolean): String = this@Node.format(table, prefix) + node.format(table, prefix)
		override fun values(table: TableStructure<*>, column: ColumnContext): Map<String, Argument> = this@Node.values(table, column) + node.values(table, column)

		override fun columnContext(table: TableStructure<*>): ColumnContext = this@Node.columnContext(table).takeIf { it.isNotEmpty() } ?: node.columnContext(table)
		override fun columns(table: TableStructure<*>): List<ColumnContext> = this@Node.columns(table) + node.columns(table)
	}
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

		return if (prefix) "\"${ if (context.size == 1) column.table.name else context.dropLast(1).joinToString(".") { it.name } }\".\"${ column.name }\""
		else "\"${ column.name }\""
	}

	override fun columnContext(table: TableStructure<*>): ColumnContext
	override fun columns(table: TableStructure<*>): List<ColumnContext> = listOf(columnContext(table))
}