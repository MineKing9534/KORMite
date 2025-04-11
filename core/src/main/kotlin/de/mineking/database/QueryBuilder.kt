package de.mineking.database

import org.jdbi.v3.core.argument.Argument

data class VariableBinding<T>(val variable: VariableNode<out T>, val value: Node<out T>)
infix fun <T> VariableNode<out T>.bindTo(value: Node<out T>) = VariableBinding(this, value)

class QueryBuilder<T>(private val table: TableImplementation<*>, private val query: (String, Map<String, Argument>, Map<String, Any?>, List<ColumnContext>) -> QueryResult<T>) : QueryResult<T> {
    private val definitions = mutableMapOf<String, Any?>()
    private val nodes = arrayListOf<Node<*>>()
    private val joins = arrayListOf<Join>()

    private var variables: VariableJoin? = null
    private var variablePosition: Int = -1

    private var limit: Int? = null
    private var offset: Int? = null
    private var order: Order? = null
    private var condition: Where = Conditions.EMPTY

    private var defaultJoins = true

    fun rawNode(node: Node<*>) {
        nodes += node
    }

    fun nodes(vararg nodes: Node<*>) = nodes(nodes.toList())
    fun nodes(nodes: Collection<Node<*>>) = apply {
        nodes.forEach {
            if (it is PropertyNode) {
                val column = it.columnContext(table.structure)
                column.last().mapper.select(this, column)
            } else rawNode(it)
        }
    }

    fun defaultNodes() = nodes(table.structure.properties.map { property(it.property) } )

    fun variables(vararg variables: VariableBinding<*>, position: Int = joins.size) = variables(variables.toList(), position)
    fun variables(variables: Collection<VariableBinding<*>>, position: Int = joins.size) = apply {
        if (this.variables != null) error("Variables already defined. Currently, multiple variable definitions are not supported.")

        this.variables = VariableJoin(variables, VARIABLE_TABLE_NAME)
        this.variablePosition = position
    }

    fun define(name: String, value: Any?) = apply { definitions[name] = value }

    fun limit(limit: Int?) = apply { this.limit = limit }
    fun offset(offset: Int?) = apply { this.offset = offset }
    fun order(order: Order?) = apply { this.order = order }
    fun where(where: Where) = apply { this.condition = where }

    fun join(table: TableStructure<*>, name: String = table.name, where: Where, index: Int = joins.size) = apply { joins.add(index, TableJoin(table, name, where)) }
    fun preventDefaultJoins() = apply { defaultJoins = false }

    private fun render() = """
        select ${ nodes.joinToString(", ") { it.format(table.structure) } }
		from "${ table.structure.name }"
        ${ joins.joinToString(" ") { "left join ${ it.format(table.structure) } as \"${ it.name }\" on ${ it.condition.format(this.table.structure) }" } }
		${ condition.formatCondition(table.structure) } 
		${ order?.formatOrder(table.structure) ?: "" } 
		${ limit?.let { "limit $it" } ?: "" }
		${ offset?.let { "offset $it" } ?: "" } 
    """.trim().replace("\\s+".toRegex(), " ")

    override fun <R> useIterator(handler: (QueryIterator<T>) -> R): R {
        if (variables != null) joins.add(variablePosition.takeIf { it >= 0 } ?: joins.size, variables!!)

        val values = joins.flatMap { it.values(table.structure).entries }.associate { it.key to it.value } +
                nodes.flatMap { it.values(table.structure, it.columnContext(table.structure)).entries }.associate { it.key to it.value } +
                condition.values(table.structure) +
                (order?.values(table.structure) ?: emptyMap())

        if (defaultJoins) {
            var index = 0
            (nodes.flatMap { it.columns(table.structure) } + condition.columns(table.structure))
                .filter { it.size > 1 }
                .map { it.dropLast(1) }
                .toSet()
                .forEach { context ->
                    val column = context.last()
                    val key = context + column.reference!!.structure.getKeys().single()

                    join(
                        column.reference!!.structure, context.joinToString(".") { it.name },
                        where = property<Any?>(key.map { it.property }) isEqualTo property<Any?>(context.map { it.property }),
                        index = index++
                    )
                }
        }

        return query(render(), values, definitions, nodes.map { it.columnContext(table.structure) }).useIterator(handler)
    }
}

sealed class Join(val name: String, val condition: Where): Node<Any?> {
    override fun values(table: TableStructure<*>, column: ColumnContext) = condition.values(table, column)
}

class TableJoin(val table: TableStructure<*>, name: String, condition: Where) : Join(name, condition) {
    override fun format(table: TableStructure<*>, prefix: Boolean) = this.table.name
}

class VariableJoin(val values: Collection<VariableBinding<*>>, name: String) : Join(name, Conditions.ALL) {
    override fun format(table: TableStructure<*>, prefix: Boolean) = "lateral (select ${ values.joinToString(", ") { "${ it.value.format(table) } as ${ it.variable.name() }" } })"
    override fun values(table: TableStructure<*>, column: ColumnContext) = super.values(table, column) + values.flatMap { it.value.values(table, column).entries }.associate { it.key to it.value }
}