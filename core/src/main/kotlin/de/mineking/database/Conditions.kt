package de.mineking.database

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

typealias Where = Node<Boolean>
object Conditions {
    val ALL = node<Boolean>("true")
    val NONE = node<Boolean>("false")
    val EMPTY = node<Boolean>("")
}

fun Where.formatCondition(table: TableStructure<*>) = format(table).takeIf { it.isNotBlank() }?.let { "where $it" } ?: ""

fun Where.combineWith(other: Where, format: (String, String) -> String) = object : Where {
    override fun format(table: TableStructure<*>, prefix: Boolean): String {
        val a = this@combineWith.format(table, prefix)
        val b = other.format(table, prefix)

        return when {
            a.isBlank() -> b
            b.isBlank() -> a
            else -> format(a, b)
        }
    }

    override fun values(table: TableStructure<*>, column: ColumnContext) = this@combineWith.values(table, column) + other.values(table, column)
    override fun columnContext(table: TableStructure<*>) = this@combineWith.columnContext(table) + other.columnContext(table)
    override fun columns(table: TableStructure<*>) = this@combineWith.columns(table) + other.columns(table)
}

infix fun Where.or(other: Where) = combineWith(other) { a, b -> "$a or $b" }
infix fun Where.and(other: Where) = combineWith(other) { a, b -> "$a and $b" }

operator fun Where.not(): Where = combineWith(Conditions.EMPTY) { a, b -> "not ($a)" }

fun combine(conditions: Collection<Where>, delimiter: String, transform: (Where) -> Where = { it }): Where {
    if (conditions.isEmpty()) return Conditions.EMPTY
    return object : Where {
        override fun format(table: TableStructure<*>, prefix: Boolean) = conditions.mapNotNull { transform(it).format(table, prefix).takeIf { it.isNotBlank() } }.joinToString(delimiter)
        override fun values(table: TableStructure<*>, column: ColumnContext) = conditions.filter { it.format(table).isNotBlank() }.flatMap { it.values(table, column).map { it.key to it.value } }.toMap()

        override fun columnContext(table: TableStructure<*>): ColumnContext {
            for (node in conditions) {
                val temp = node.columnContext(table)
                if (temp.isNotEmpty()) return temp
            }

            return emptyList()
        }

        override fun columns(table: TableStructure<*>) = conditions.flatMap { it.columns(table) }
    }
}

fun allOf(vararg conditions: Where) = allOf(arrayListOf(*conditions))
fun allOf(conditions: Collection<Where>) = combine(conditions, "and")

fun anyOf(vararg conditions: Where) = anyOf(arrayListOf(*conditions))
fun anyOf(conditions: Collection<Where>) = combine(conditions, "or")

fun noneOf(vararg conditions: Where) = noneOf(arrayListOf(*conditions))
fun noneOf(conditions: Collection<Where>) = combine(conditions, "and") { !it }

fun <T: Any> identifyObject(table: TableStructure<T>, obj: T): Where {
    val keys = table.getKeys()
    require(keys.isNotEmpty()) { "Cannot identify object without keys" }

    return allOf(keys.map { unsafe(it.name) isEqualTo value(it.get(obj), it.type) })
}

@Suppress("UNCHECKED_CAST")
fun createCondition(node: Node<*>) = object : Where {
    override fun format(table: TableStructure<*>, prefix: Boolean) = node.format(table, prefix)
    override fun values(table: TableStructure<*>, column: ColumnContext) = node.values(table, column.takeIf { it.isNotEmpty() } ?: columnContext(table))

    override fun columnContext(table: TableStructure<*>) = node.columnContext(table)
    override fun columns(table: TableStructure<*>) = node.columns(table)
}

infix fun Node<*>.isEqualTo(other: Node<*>) = createCondition(this + " = " + other)
infix fun Node<*>.isNotEqualTo(other: Node<*>) = createCondition(this + " != " + other)

infix fun Node<*>.isLike(other: Node<String>) = createCondition(this + " like " + other)
infix fun Node<*>.isLikeIgnoreCase(other: Node<String>) = createCondition(this + " ilike " + other)

fun Node<*>.isIn(vararg nodes: Node<*>) = isIn(nodes.toList())
fun Node<*>.isIn(nodes: Collection<Node<*>>) = createCondition(this + " in (" + nodes.join() + ")")

infix fun Node<*>.isGreaterThan(other: Node<*>) = createCondition(this + " > " + other)
infix fun Node<*>.isGreaterThanOrEqual(other: Node<*>) = createCondition(this + " >= " + other)

infix fun Node<*>.isLowerThan(other: Node<*>) = createCondition(this + " < " + other)
infix fun Node<*>.isLowerThanOrEqual(other: Node<*>) = createCondition(this + " <= " + other)

fun Node<*>.isBetween(a: Node<*>, b: Node<*>) = createCondition(this + " between " + a + " and " + b)

fun Node<*>.isNull() = createCondition(this + " is null")
fun Node<*>.isNotNull() = createCondition(this + " is not null")