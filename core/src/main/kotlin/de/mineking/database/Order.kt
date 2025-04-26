package de.mineking.database

import kotlin.reflect.KProperty

@Suppress("UNCHECKED_CAST")
class Order(val node: Node<*>) : Node<Any?> by node as Node<Any?>

fun descendingBy(node: Node<*>): Order = Order(node + " desc")
fun descendingBy(property: KProperty<*>) = descendingBy(property(property))

fun ascendingBy(node: Node<*>): Order = Order(node + " asc")
fun ascendingBy(property: KProperty<*>) = ascendingBy(property(property))

infix fun Order.andThen(other: Order): Order = Order(this + ", " + other)

fun orderBy(orders: List<Order>): Order? {
    var result = null as Order?
    orders.forEach {
        if (result == null) result = it
        else result = result andThen it
    }
    return result
}
fun orderBy(vararg orders: Order) = orderBy(orders.asList())

fun Order.formatOrder(table: TableStructure<*>) = format(table).takeIf { it.isNotBlank() }?.let { "order by $it" } ?: ""

@Suppress("UNCHECKED_CAST")
inline fun <reified T : Enum<T>> Node<T>.enumPosition() = "array_position"(value(T::class.java.enumConstants), this) as Node<Int>