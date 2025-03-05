package de.mineking.database

import de.mineking.database.Where.Companion.combine
import org.jdbi.v3.core.argument.Argument
import kotlin.reflect.KProperty

fun interface Order {
    fun get(): String
    fun format(): String = get().takeIf { it.isNotBlank() }?.let { "order by $it" } ?: ""

    infix fun andThen(other: Order): Order = Order { "${ this.get() }, ${ other.get() }" }
}

fun ascendingBy(name: String) = Order { "\"$name\" asc" }
fun ascendingBy(property: KProperty<*>) = ascendingBy(property.name)

fun descendingBy(name: String) = Order { "\"$name\" desc" }
fun descendingBy(property: KProperty<*>) = descendingBy(property.name)

interface Where : Node<Any?> {
    companion object {
        val ALL = unsafe("true")
        val NONE = unsafe("false")
        val EMPTY = unsafe("")

        fun combine(string: (TableStructure<*>) -> String, vararg components: Where) = object : Where {
            override fun get(table: TableStructure<*>): String = string(table)
            override fun values(table: TableStructure<*>): Map<String, Argument> = components.filter { it.get(table).isNotBlank() }.flatMap { it.values(table).map { it.key to it.value } }.toMap()
        }
    }

    fun get(table: TableStructure<*>): String
    fun format(table: TableStructure<*>): String = get(table).takeIf { it.isNotBlank() }?.let { "where $it" } ?: ""

    fun values(table: TableStructure<*>): Map<String, Argument> = emptyMap()

    override fun format(table: TableStructure<*>, formatter: (ColumnInfo) -> String) = format(table)
    override fun values(table: TableStructure<*>, column: ColumnData<*, *>?) = values(table)
}

infix fun Where.or(other: Where): Where = combine({ when {
    get(it).isBlank() -> other.get(it)
    other.get(it).isBlank() -> get(it)
    else -> "(${ get(it) }) or (${ other.get(it) })"
} }, this, other)

infix fun Where.and(other: Where): Where = combine({ when {
    get(it).isBlank() -> other.get(it)
    other.get(it).isBlank() -> get(it)
    else -> "(${ get(it) }) and (${ other.get(it) })"
} }, this, other)

operator fun Where.not(): Where = combine({ "not (${ get(it) })" }, this)

fun unsafe(string: String) = object : Where { override fun get(table: TableStructure<*>) = string }

fun allOf(vararg conditions: Where): Where = allOf(arrayListOf(*conditions))
fun allOf(conditions: Collection<Where>): Where = if (conditions.isEmpty()) Where.EMPTY else object : Where {
    override fun get(table: TableStructure<*>): String = conditions.filter { it.get(table).isNotBlank() }.joinToString(" and ") { "(${it.get(table)})" }
    override fun values(table: TableStructure<*>): Map<String, Argument> = conditions.filter { it.get(table).isNotBlank() }.flatMap { it.values(table).map { it.key to it.value } }.toMap()
}

fun anyOf(vararg conditions: Where): Where = anyOf(arrayListOf(*conditions))
fun anyOf(conditions: Collection<Where>): Where = if (conditions.isEmpty()) Where.NONE else object : Where {
    override fun get(table: TableStructure<*>): String = conditions.filter { it.get(table).isNotBlank() }.joinToString(" or ") { "(${it.get(table)})" }
    override fun values(table: TableStructure<*>): Map<String, Argument> = conditions.filter { it.get(table).isNotBlank() }.flatMap { it.values(table).map { it.key to it.value } }.toMap()
}

fun noneOf(vararg conditions: Where): Where = noneOf(arrayListOf(*conditions))
fun noneOf(conditions: Collection<Where>): Where = if (conditions.isEmpty()) Where.EMPTY else object : Where {
    override fun get(table: TableStructure<*>): String = conditions.filter { it.get(table).isNotBlank() }.joinToString(" and ") { "not (${it.get(table)})" }
    override fun values(table: TableStructure<*>): Map<String, Argument> = conditions.filter { it.get(table).isNotBlank() }.flatMap { it.values(table).map { it.key to it.value } }.toMap()
}

fun <T: Any> identifyObject(table: TableStructure<T>, obj: T): Where {
    val keys = table.getKeys()
    require(keys.isNotEmpty()) { "Cannot identify object without keys" }

    return allOf(keys.map { unsafeNode(it.name) isEqualTo value(it.get(obj), it.type) })
}

fun Where(node: Node<*>): Where = object : Where {
    override fun get(table: TableStructure<*>): String = node.format(table)
    override fun values(table: TableStructure<*>): Map<String, Argument> = node.values(table, node.columnContext(table)?.column)
}

infix fun Node<*>.isEqualTo(other: Node<*>) = Where(this + " = " + other)
infix fun Node<*>.isNotEqualTo(other: Node<*>) = Where(this + " != " + other)

infix fun Node<*>.isLike(other: Node<String>) = Where(this + " like " + other)
infix fun Node<*>.isLikeIgnoreCase(other: Node<String>) = Where(this + " ilike " + other)

fun Node<*>.isIn(nodes: Array<Node<*>>) = Where(this + " in (" + nodes.join() + ")")
fun Node<*>.isIn(nodes: Collection<Node<*>>) = isIn(nodes.toTypedArray())

infix fun Node<*>.isGreaterThan(other: Node<*>) = Where(this + " > " + other)
infix fun Node<*>.isGreaterThanOrEqual(other: Node<*>) = Where(this + " >= " + other)

infix fun Node<*>.isLowerThan(other: Node<*>) = Where(this + " < " + other)
infix fun Node<*>.isLowerThanOrEqual(other: Node<*>) = Where(this + " <= " + other)

fun Node<*>.isBetween(a: Node<*>, b: Node<*>) = Where(this + " between " + a + " and " + b)

fun Node<*>.isNull(): Where = Where(this + " is null")
fun Node<*>.isNotNull(): Where = Where(this + " is not null")