package de.mineking.database

import org.jdbi.v3.core.argument.Argument
import kotlin.reflect.KProperty
import kotlin.reflect.KProperty1
import kotlin.reflect.KType
import kotlin.reflect.full.isSubtypeOf
import kotlin.reflect.jvm.jvmErasure
import kotlin.reflect.typeOf

fun Array<out Node<*>>.join(delimiter: Node<*> = unsafeNode(", ")): Node<*> {
	var result = Node.EMPTY

	val iterator = iterator()
	while (iterator.hasNext()) {
		val node = iterator.next()
		result += node

		if (iterator.hasNext()) result += delimiter
	}

	return result
}

operator fun String.invoke(vararg params: Node<*>) = unsafeNode(this) + "(" + params.join() + ")"

@Suppress("UNCHECKED_CAST")
fun Node<String>.length() = "length"(this) as Node<Int>

@Suppress("UNCHECKED_CAST")
fun Node<String>.lowercase() = "lower"(this) as Node<String>

@Suppress("UNCHECKED_CAST")
fun Node<String>.uppercase() = "upper"(this) as Node<String>

fun abs(node: Node<Number>) = "abs"(node)

fun query(query: String) = Node.EMPTY + "(" + query + ")"

fun <T> property(name: String): PropertyNode<T> = object : PropertyNode<T> {
	override fun format(table: TableStructure<*>, formatter: (ColumnInfo) -> String): String = formatter(parseColumnSpecification(name, table))
	override fun columnContext(table: TableStructure<*>): ColumnInfo = parseColumnSpecification(name, table)
}

fun <T> property(property: KProperty<*>, vararg reference: KProperty<*>) = property<T>((arrayOf(property) + reference).joinToString("->") { it.name })
fun <T> property(property: KProperty<T>) = property<T>(property, *emptyArray())
fun <T, I> property(property: KProperty1<*, I>, reference: KProperty1<I, T>) = property<T>(property, reference)
fun <T, I1, I2> property(property: KProperty1<*, I1>, reference1: KProperty1<I1, I2>, reference2: KProperty1<I2, T>) = property<T>(property, reference1, reference2)

inline fun <reified T> valueForProperty(property: KProperty<T>, value: T, static: Boolean = false) = valueForProperty(property, value, typeOf<T>(), static)
fun <T> valueForProperty(property: KProperty<T>, value: T, type: KType, static: Boolean = false): PropertyNode<T> = object : PropertyNode<T>, ValueNode<T> {
	override fun format(table: TableStructure<*>, formatter: (ColumnInfo) -> String): String = ":${ hashCode() }"

	override fun values(table: TableStructure<*>, column: ColumnData<*, *>?): Map<String, Argument> {
		@Suppress("UNCHECKED_CAST")
		val manager = column?.takeIf { !static && it.type.isSubtypeOf(type) }?.mapper as TypeMapper<T, *>? ?: table.manager.getTypeMapper<T, Any>(type, column?.getRootColumn()?.property?.takeIf { !static }) ?: throw IllegalArgumentException("Cannot find suitable TypeMapper for $type")
		return mapOf(hashCode().toString() to manager.write(column, table, type, value))
	}

	override fun columnContext(table: TableStructure<*>): ColumnInfo = parseColumnSpecification(property.name, table)
}

fun <T> value(value: T, type: KType, static: Boolean = false): ValueNode<T> = object : ValueNode<T> {
	override fun format(table: TableStructure<*>, formatter: (ColumnInfo) -> String): String = ":${ hashCode() }"

	override fun values(table: TableStructure<*>, column: ColumnData<*, *>?): Map<String, Argument> {
		@Suppress("UNCHECKED_CAST")
		val manager = column?.takeIf { !static && it.type.isSubtypeOf(type) }?.mapper as TypeMapper<T, *>? ?: table.manager.getTypeMapper<T, Any>(type, column?.getRootColumn()?.property?.takeIf { !static }) ?: throw IllegalArgumentException("Cannot find suitable TypeMapper for $type")
		return mapOf(hashCode().toString() to manager.write(column, table, type, value))
	}
}

inline fun <reified T> value(value: T, static: Boolean = false) = value(value, typeOf<T>(), static)

fun <T> nullValue() = object : ValueNode<T?> {
	override fun format(table: TableStructure<*>, formatter: (ColumnInfo) -> String): String = "null"
	override fun values(table: TableStructure<*>, column: ColumnData<*, *>?): Map<String, Argument> = emptyMap()
}

fun unsafeNode(string: String, values: Map<String, Argument> = emptyMap()) = object : Node<Any?> {
	override fun format(table: TableStructure<*>, formatter: (ColumnInfo) -> String): String = string
	override fun values(table: TableStructure<*>, column: ColumnData<*, *>?): Map<String, Argument> = values
}

interface Node<T> {
	companion object {
		val EMPTY = unsafeNode("")
	}

	fun format(table: TableStructure<*>, formatter: (ColumnInfo) -> String = { it.build() }): String
	fun values(table: TableStructure<*>, column: ColumnData<*, *>?): Map<String, Argument>

	fun columnContext(table: TableStructure<*>): ColumnInfo? = null

	@Suppress("UNCHECKED_CAST")
	operator fun plus(string: String): Node<T> = (this + unsafeNode(string)) as Node<T>
	operator fun plus(node: Node<*>): Node<Any?> = object : Node<Any?> {
		override fun format(table: TableStructure<*>, formatter: (ColumnInfo) -> String): String = this@Node.format(table, formatter) + node.format(table, formatter)
		override fun values(table: TableStructure<*>, column: ColumnData<*, *>?): Map<String, Argument> = this@Node.values(table, column) + node.values(table, column)
		override fun columnContext(table: TableStructure<*>): ColumnInfo? = this@Node.columnContext(table) ?: node.columnContext(table)
	}
}

interface ValueNode<T> : Node<T>
interface PropertyNode<T> : Node<T> {
	override fun values(table: TableStructure<*>, column: ColumnData<*, *>?): Map<String, Argument> = emptyMap()
}

data class ColumnInfo(val column: ColumnData<*, *>, val context: Array<String> = emptyArray(), val transform: (String) -> String = { "\"$it\"" }) {
	fun build(prefix: Boolean = true): String = "${ if (prefix) "\"${ context.takeIf { it.isNotEmpty() }?.joinToString(".") ?: column.table.name }\"." else "" }${ transform(column.name) }"
}

fun <T: Any> parseColumnSpecification(name: String, table: TableStructure<T>, columnFinder: (String) -> ColumnData<T, *>? = table::getColumnFromCode): ColumnInfo = when {
	name.matches(".*\\[.+?]$".toRegex()) -> {
		val node = name.replace(".*\\[(.+?)]$".toRegex(), "$1").let { it.toIntOrNull()?.let { it + 1 } ?: "${ parseColumnSpecification(it, table, columnFinder).build() } + 1" }
		parseColumnSpecification(name.replace("(.*)\\[.+?]$".toRegex(), "$1"), table, columnFinder).copy(transform = { "\"$it\"[$node]" })
	}

	"->" in name -> {
		val parts = name.split("->", limit = 2)
		require(parts.size == 2) { "Illegal property name format" }

		val context = parseColumnSpecification(parts[0], table).column
		require(context is DirectColumnData) { "Reference context has to be a direct column!" }

		require(context.property.hasDatabaseAnnotation<Reference>()) { "Column ${ context.name } is not an external reference" }

		val (child, childContext) = parseColumnSpecification(parts[1], table.manager.getTableStructure(context.type.componentIfArray().jvmErasure, ""))
		ColumnInfo(child, arrayOf(context.name) + childContext)
	}

	"." in name -> {
		val parts = name.split(".", limit = 2)
		require(parts.size == 2) { "Illegal property name format" }

		val column = parseColumnSpecification(parts[0], table).column
		require(column is DirectColumnData) { "Virtual host has to be a direct column!" }

		@Suppress("UNCHECKED_CAST")
		val child = parseColumnSpecification(parts[1], table) { name -> column.getChildren().find { it.simpleName == name } as ColumnData<T, *> }.column as VirtualColumnData

		child.transform?.let { ColumnInfo(child, transform = it) } ?: ColumnInfo(child)
	}

	else -> {
		val column = columnFinder(name) ?: throw IllegalArgumentException("Column $name not found in ${ table.name }")
		ColumnInfo(column)
	}
}