package de.mineking.database

import org.jdbi.v3.core.argument.Argument
import org.jdbi.v3.core.kotlin.useHandleUnchecked
import org.jdbi.v3.core.statement.Update
import java.lang.reflect.InvocationHandler
import java.lang.reflect.InvocationTargetException
import java.lang.reflect.Method
import kotlin.reflect.KClass
import kotlin.reflect.KProperty
import kotlin.reflect.KType
import kotlin.reflect.full.createType
import kotlin.reflect.full.functions
import kotlin.reflect.full.valueParameters
import kotlin.reflect.jvm.jvmErasure
import kotlin.reflect.jvm.kotlinFunction
import kotlin.reflect.typeOf

abstract class TableImplementation<T: Any>(
    val tableType: KClass<*>,
    override val structure: TableStructure<T>,
    val instance: () -> T
) : DefaultTable<T>, InvocationHandler {
    override val implementation: TableImplementation<T> = this

    abstract fun createTable()
    fun dropTable() = structure.manager.driver.useHandleUnchecked { it.createUpdate("drop table ${ structure.name }").execute() }

    val mapper = object : TypeMapper<T?, Any?> {
        override fun accepts(manager: DatabaseConnection, property: KProperty<*>?, type: KType): Boolean = property?.getDatabaseAnnotation<Reference>()?.table == structure.name && type.jvmErasure == structure.component

        override fun getType(column: PropertyData<*, *>?, table: TableStructure<*>, type: KType): DataType {
            if (structure.getKeys().size != 1) error("Can only reference type with exactly one key")

            val key = structure.getKeys().first()
            return key.mapper.getType(column, table, key.type).withNullability(type.isMarkedNullable)
        }

        override fun <O : Any> initialize(column: PropertyData<O, *>, type: KType) {
            column.reference = this@TableImplementation
        }

        override fun select(query: QueryBuilder<*>, context: ColumnContext) {
            super.select(query, context)
            structure.properties.forEach { query.nodes(property<Any?>((context + it).map { it.property })) }
        }

        override fun format(column: ColumnContext, table: TableStructure<*>, type: KType, value: T?): Any? {
            if (structure.getKeys().size != 1) error("Can only reference type with exactly one key")

            @Suppress("UNCHECKED_CAST")
            fun <C> format(key: PropertyData<T, C>) = key.mapper.format(column + key, table, type, value?.let { key.get(it) } as C)
            return format(structure.getKeys().first())
        }

        override fun extract(column: ColumnContext, type: KType, context: ReadContext, pos: Int): Any? {
            if (context.currentContext.isEmpty()) return null

            if (structure.getKeys().size != 1) error("Can only reference type with exactly one key")

            fun <C> extract(key: PropertyData<T, C>) = key.mapper.extract(column + key, type, context, pos)
            return extract(structure.getKeys().first())
        }

        override fun parse(column: ColumnContext, type: KType, value: Any?, context: ReadContext, pos: Int): T? {
            @Suppress("UNCHECKED_CAST")
            val instance = context.instance as T? ?: instance()

            structure.properties.forEach {
                val columnContext = context.currentContext + it
                val index = context.columns.lastIndexOf(columnContext) //Use last index of to allow overriding
                if (index == -1) return@forEach

                fun <C> set(column: PropertyData<T, C>): Boolean {
                    val value = column.mapper.read(columnContext, column.type, context.nest(column), index + 1)

                    if (column is ColumnData && column.key && value == null) return false

                    column.set(instance, value)
                    return true
                }

                //If key is null parsing is immediately terminated
                if (!set(it)) return@parse null
            }

            return instance
        }

        override fun toString() = "RowTypeMapper[${ structure.component.simpleName } in ${ structure.name }]"
    }

    fun query() = QueryBuilder<T>(this) { sql, values, columns -> query(sql, values, columns = columns) }
    fun <T> query(type: KType, mapper: TypeMapper<T, *>, position: Int = 1, column: ColumnContext = emptyList()) = QueryBuilder<T>(this) { sql, values, columns -> query(sql, type, mapper, values, emptyMap(), position, column, columns) }
    inline fun <reified T> query(position: Int = 1, column: ColumnContext = emptyList()): QueryBuilder<T> {
        val mapper = structure.manager.getTypeMapper<T, Any?>(typeOf<T>(), column.lastOrNull()?.property)
        return query(typeOf<T>(), mapper, position, column)
    }

    @Suppress("UNCHECKED_CAST")
    fun query(sql: String, parameters: Map<String, Any?> = emptyMap(), definitions: Map<String, Any?> = emptyMap(), columns: List<ColumnContext> = emptyList()): QueryResult<T> = query(sql, structure.component.createType(), mapper as TypeMapper<T, *>, parameters, definitions, columns = columns)
    fun <T> query(sql: String, type: KType, mapper: TypeMapper<T, *>, parameters: Map<String, Any?> = emptyMap(), definitions: Map<String, Any?> = emptyMap(), position: Int = 1, column: ColumnContext = emptyList(), columns: List<ColumnContext> = emptyList()) = object : QueryResult<T> {
        override fun <R> useIterator(handler: (QueryIterator<T>) -> R): R = structure.manager.execute {
            handler(it.createQuery(sql)
                .bindMap(parameters)
                .apply {
                    define("TABLE", structure.name)
                    definitions.forEach { define(it.key, it.value) }
                }
                .scanResultSet { set, ctx -> QueryIterator<T>(ReadContext(this@TableImplementation, set.get(), columns, column), ctx, position, column, type, mapper) }
            )
        }
    }

    inline fun <reified T> query(sql: String, parameters: Map<String, Any?> = emptyMap(), definitions: Map<String, Any?> = emptyMap(), position: Int = 1, column: ColumnContext = emptyList(), columns: List<ColumnContext> = emptyList()): QueryResult<T> {
        val mapper = structure.manager.getTypeMapper<T, Any?>(typeOf<T>(), column.lastOrNull()?.property)
        return query(sql, typeOf<T>(), mapper, parameters, definitions, position, column, columns)
    }

    override fun selectRowCount(where: Node<Boolean>): Int {
        return query(typeOf<Int>(), structure.manager.getTypeMapper<Int>())
            .nodes(unsafe("count(*)"))
            .where(where)
            .first()
    }

    override fun select(vararg columns: Node<*>, where: Node<Boolean>, order: Order?, limit: Int?, offset: Int?): QueryResult<T> {
        return query()
            .apply { if (columns.isEmpty()) defaultNodes() else nodes(*columns) }
            .where(where)
            .limit(limit)
            .offset(offset)
            .order(order)
    }

    override fun <C> selectValue(target: Node<C>, type: KType, where: Node<Boolean>, order: Order?, limit: Int?, offset: Int?): QueryResult<C> {
        val column = target.columnContext(structure)
        val mapper = structure.manager.getTypeMapper<C, Any>(type, column.lastOrNull()?.property)

        return query<C>(type, mapper, 1, column = column)
            .nodes(target)
            .where(where)
            .limit(limit)
            .offset(offset)
            .order(order)
    }

    private fun executeUpdate(update: Update, obj: T): T {
        structure.columns.forEach {
            fun <C> createArgument(column: ColumnData<T, C>) = column.mapper.write(listOf(column), structure, column.type, column.get(obj))
            update.bind(it.name, createArgument(it))
        }

        return update.execute { statement, ctx ->
            val set = statement.get().resultSet

            @Suppress("UNCHECKED_CAST")
            QueryIterator<T>(ReadContext(this, set, structure.columns.filter { it.autogenerate }.map { listOf(it) }, instance = obj), ctx, Int.MIN_VALUE, emptyList(), structure.component.createType(), mapper as TypeMapper<T, *>).next()
        }
    }

    abstract fun <T> createResult(function: () -> T): UpdateResult<T>

    override fun update(obj: T): UpdateResult<T> {
        if (obj is DataObject<*>) obj.beforeWrite()
        val identity = identifyObject(obj)

        val columns = structure.columns.filter { !it.key }

        val sql = """
			update ${ structure.name }
			set ${ columns.joinToString { "\"${ it.name }\" = :${ it.name }" } }
			${ identity.formatCondition(structure) }
			returning *
		""".trim().replace("\\s+".toRegex(), " ")

        return createResult {
            val result = structure.manager.execute { executeUpdate(it.createUpdate(sql).bindMap(identity.values(structure)), obj) }
            result.apply { if (this is DataObject<*>) afterRead() }
        }
    }

    override fun update(vararg columns: Pair<Node<*>, Node<*>>, where: Node<Boolean>): UpdateResult<Int > {
        val specs = columns.associate { (column, value) ->
            (column to (column.columnContext(structure).takeIf { it.isNotEmpty() } ?: error("Update node has to reference a property"))) to (value to value.columnContext(structure))
        }

        require(specs.none { (column) -> column.second.size > 1 }) { "Cannot update reference property, update reference table directly" }
        require(specs.none { (column) ->
            val c = column.second.last()
            c is ColumnData && c.key
        }) { "Cannot update key" }

        val sql = """
			update ${ structure.name } 
			set ${ specs.map { (column, value) -> column.first.buildUpdate(structure, value.first) }.join().format(structure) }
			${ where.formatCondition(structure) } 
		""".trim().replace("\\s+".toRegex(), " ")

        return createResult { structure.manager.execute { it.createUpdate(sql)
            .bindMap(specs.flatMap { (column, value) -> column.first.values(structure, column.second).entries + value.first.values(structure, value.second.takeIf { it.isNotEmpty() } ?: column.second).entries }.associate { it.toPair() })
            .bindMap(where.values(structure))
            .execute()
        } }
    }

    private fun insertColumns(obj: T) = structure.columns.filter {
        if (!it.autogenerate) true
        else {
            val value = it.get(obj)
            value != 0 && value != null //TODO ideally this should be moved to annotation handlers
        }
    }

    override fun insert(obj: T): UpdateResult<T> {
        if (obj is DataObject<*>) obj.beforeWrite()

        val columns = insertColumns(obj)

        val sql = """
			insert into ${ structure.name }
			(${ columns.joinToString { "\"${ it.name }\"" } })
			values(${ columns.joinToString { ":${ it.name }" } }) 
			returning *
		""".trim().replace("\\s+".toRegex(), " ")

        return createResult {
            val result = structure.manager.execute { executeUpdate(it.createUpdate(sql), obj) }
            result.apply { if (this is DataObject<*>) afterRead() }
        }
    }

    override fun upsert(obj: T): UpdateResult<T> {
        if (obj is DataObject<*>) obj.beforeWrite()

        val insertColumns = insertColumns(obj)

        val updateColumns = structure.columns.filter { !it.key }

        val sql = """
			insert into ${ structure.name }
			(${ insertColumns.joinToString { "\"${ it.name }\"" } })
			values(${ insertColumns.joinToString { ":${ it.name }" } }) 
			on conflict (${ structure.getKeys().joinToString { "\"${ it.name }\"" } }) do update set
			${ updateColumns.joinToString { "\"${ it.name }\" = :${ it.name }" } }
			returning *
		""".trim().replace("\\s+".toRegex(), " ")

        return createResult {
            val result = structure.manager.execute { executeUpdate(it.createUpdate(sql), obj) }
            result.apply { if (this is DataObject<*>) afterRead() }
        }
    }

    override fun delete(where: Node<Boolean>): Int {
        val sql = "delete from ${ structure.name } ${ where.formatCondition(structure) }"
        return structure.manager.execute { it.createUpdate(sql)
            .bindMap(where.values(structure))
            .execute()
        }
    }

    override fun invoke(proxy: Any?, method: Method?, args: Array<out Any?>?): Any? {
        require(method != null)
        val args = args ?: emptyArray()

        val annotationHandler = structure.manager.annotationHandlers.find { it.accepts(this, method, args) }
        if (annotationHandler != null) {
            ANNOTATION_EXECUTOR.set { annotationHandler.execute(this, it, method, args) }
            return invokeDefault(tableType, method, proxy, args) {
                execute(tableType.functions.firstOrNull {
                    it.name == method.name && it.valueParameters.map { p -> p.type } == method.kotlinFunction?.valueParameters?.map { p -> p.type }
                }!!.returnType)
            }
        }

        return try {
            javaClass.getMethod(method.name, *method.parameterTypes).invoke(this, *args)
        } catch(_: NoSuchMethodException) {
            invokeDefault(tableType, method, proxy, args) { throw RuntimeException(it) }
        } catch(e: IllegalAccessException) {
            throw RuntimeException(e)
        } catch(e: InvocationTargetException) {
            throw e.cause!!
        }
    }
}

private fun invokeDefault(type: KClass<*>, method: Method, instance: Any?, args: Array<out Any?>, default: (e: NoSuchMethodException) -> Any?): Any? {
    return try {
        type.java.classes.find { it.simpleName == "DefaultImpls" }?.getMethod(method.name, type.java, *method.parameterTypes)?.invoke(null, instance, *args)
    } catch (e: IllegalAccessException) {
        throw RuntimeException(e)
    } catch (e: NoSuchMethodException) {
        default(e)
    } catch (e: InvocationTargetException) {
        throw e.cause!!
    }
}

class QueryBuilder<T>(private val table: TableImplementation<*>, private val query: (String, Map<String, Argument>, List<ColumnContext>) -> QueryResult<T>) : QueryResult<T> {
    private val nodes: MutableList<Node<*>> = arrayListOf()
    private val joins: MutableList<Pair<Pair<TableStructure<*>, String>, Node<Boolean>>> = arrayListOf()

    private var limit: Int? = null
    private var offset: Int? = null
    private var order: Order? = null
    private var condition: Node<Boolean> = Conditions.EMPTY

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

    fun limit(limit: Int?) = apply { this.limit = limit }
    fun offset(offset: Int?) = apply { this.offset = offset }
    fun order(order: Order?) = apply { this.order = order }
    fun where(where: Node<Boolean>) = apply { this.condition = where }

    fun join(table: TableStructure<*>, name: String = table.name, where: Node<Boolean>) = apply { joins += (table to name) to where }
    fun preventDefaultJoins() = apply { defaultJoins = false }

    private fun render() = """
        select ${ nodes.joinToString(", ") { it.format(table.structure) } }
		from ${ table.structure.name }
        ${ joins.joinToString(" ") { (table, condition) -> "left join ${ table.first.name } as \"${ table.second }\" on ${ condition.format(this.table.structure) }" } }
		${ condition.formatCondition(table.structure) } 
		${ order?.format() ?: "" } 
		${ limit?.let { "limit $it" } ?: "" }
		${ offset?.let { "offset $it" } ?: "" } 
    """.trim().replace("\\s+".toRegex(), " ")

    override fun <R> useIterator(handler: (QueryIterator<T>) -> R): R {
        val values = joins.flatMap { (_, condition) -> condition.values(table.structure).entries }.associate { it.key to it.value } +
                nodes.flatMap { it.values(table.structure, it.columnContext(table.structure)).entries }.associate { it.key to it.value } +
                condition.values(table.structure)

        if (defaultJoins) {
            (nodes.flatMap { it.columns(table.structure) } + condition.columns(table.structure))
                .filter { it.size > 1 }
                .map { it.dropLast(1) }
                .toSet()
                .forEach { context ->
                    val column = context.last()
                    val key = context + column.reference!!.structure.getKeys().single()

                    join(column.reference!!.structure, context.joinToString(".") { it.name }, where = property<Any?>(key.map { it.property }) isEqualTo property<Any?>(context.map { it.property }))
                }
        }

        return query(render(), values, nodes.map { it.columnContext(table.structure) }).useIterator(handler)
    }
}