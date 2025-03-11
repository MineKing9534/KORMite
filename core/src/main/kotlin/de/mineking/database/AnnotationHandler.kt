package de.mineking.database

import java.lang.reflect.Method
import kotlin.reflect.KType
import kotlin.reflect.full.findAnnotation
import kotlin.reflect.full.hasAnnotation
import kotlin.reflect.full.valueParameters
import kotlin.reflect.jvm.jvmErasure
import kotlin.reflect.jvm.kotlinFunction
import kotlin.reflect.typeOf

val ANNOTATION_EXECUTOR = ThreadLocal<(type: KType) -> Any?>()

@Suppress("UNCHECKED_CAST")
fun <T> execute(type: KType): T = ANNOTATION_EXECUTOR.get()(type) as T
inline fun <reified T> execute(): T = execute(typeOf<T>())

interface AnnotationHandler {
    fun accepts(table: TableImplementation<*>, method: Method, args: Array<out Any?>): Boolean
    fun execute(table: TableImplementation<*>, type: KType, method: Method, args: Array<out Any?>): Any?
}

inline fun <reified A: Annotation> annotationHandler(
    crossinline execute: TableImplementation<*>.(type: KType, method: Method, args: Array<out Any?>, annotation: A) -> Any?
): AnnotationHandler = object : AnnotationHandler {
    override fun accepts(table: TableImplementation<*>, method: Method, args: Array<out Any?>) = method.isAnnotationPresent(A::class.java)
    override fun execute(table: TableImplementation<*>, type: KType, method: Method, args: Array<out Any?>) = execute(table, type, method, args, method.getAnnotation(A::class.java)!!)
}

object DefaultAnnotationHandlers {
    fun createCondition(method: Method, args: Array<out Any?>) = allOf(method.parameters
        .mapIndexed { index, value -> index to value }
        .filter { (_, it) -> it.isAnnotationPresent(Condition::class.java) }
        .map { (index, param) ->
            property<Any>(param.getAnnotation(Condition::class.java)!!.name.takeIf { it.isNotBlank() } ?: param.name) +
            param.getAnnotation(Condition::class.java)!!.operation +
            value(args[index], method.kotlinFunction!!.valueParameters[index].type)
        }
        .map { createCondition(it) }
    )

    @Suppress("UNCHECKED_CAST")
    fun <T: Any> createObject(table: TableImplementation<T>, method: Method, args: Array<out Any?>): T {
        val obj = table.instance()

        method.kotlinFunction!!.valueParameters
            .mapIndexed { index, value -> index to value }
            .filter { (_, it) -> it.hasAnnotation<Parameter>() }
            .forEach { (index, param) ->
                val name = param.findAnnotation<Parameter>()!!.name.takeIf { it.isNotBlank() } ?: param.name!!

                val column = table.structure.getFromCode(name) as PropertyData<Any, Any?>? ?: error("Column $name not found")
                val value = args[index]

                try {
                    //Try direct
                    column.set(obj, value)
                } catch(_: IllegalArgumentException) {
                    //Try parsing with column mapper
                    column.set(obj, (column.mapper as TypeMapper<*, Any?>).parse(listOf(column), param.type, value, ReadContext(table, createDummy(), emptyList()), 0))
                } catch(_: IllegalArgumentException) {
                    //Try formatting with value mapper
                    column.set(obj, table.structure.manager.getTypeMapper<Any?, Any>(param.type, null).format(listOf(column), table.structure, param.type, value))
                }
            }

        return obj
    }

    val QUERY = annotationHandler<Query> { type, function, args, annotation ->
        val parameters = function.kotlinFunction!!.valueParameters
            .mapIndexed { index, value -> index to value }
            .filter { (_, it) -> it.hasAnnotation<Parameter>() }
            .associate { (index, param) ->
                val name = param.findAnnotation<Parameter>()!!.name.takeIf { it.isNotBlank() } ?: param.name!!
                val value = args[index]

                val mapper = structure.manager.getTypeMapper<Any?, Any?>(param.type, null)
                name to mapper.write(emptyList(), structure, param.type, value)
            }

        val definitions = function.parameters
            .mapIndexed { index, value -> index to value }
            .filter { (_, it) -> it.isAnnotationPresent(Define::class.java) }
            .associate { (index, param) ->
                (param.getAnnotation(Parameter::class.java)!!.name.takeIf { it.isNotBlank() } ?: param.name!!) to args[index]
            }

        val queryType = when (type.jvmErasure) {
            QueryResult::class, List::class, Set::class -> type.arguments[0].type!!
            else -> type
        }

        val mapper = if (queryType.jvmErasure == structure.component) mapper else structure.manager.getTypeMapper<Any?, Any?>(queryType, null)
        val columns =
            if (queryType.jvmErasure == structure.component) {
                if (annotation.columns.isEmpty()) structure.properties.map { listOf(it) }
                else annotation.columns.map { structure.getFromCode(it) ?: error("Column $it not found") }.map { listOf(it) }
            } else emptyList()

        val value = query(annotation.sql, queryType, mapper, parameters = parameters, definitions = definitions, position = annotation.position, columns = columns)

        when (type.jvmErasure) {
            QueryResult::class -> value
            List::class -> value.list()
            Set::class -> value.set()
            queryType.jvmErasure -> if (queryType.isMarkedNullable) value.firstOrNull() else value.first()
            else -> error("Cannot produce $type as result")
        }
    }

    val SELECT = annotationHandler<Select> { type, function, args, _ ->
        val (limit, offset, order, condition) = queryWindow(function, args)

        val value = select(where = createCondition(function, args) and condition, order = order, limit = limit, offset = offset)
        when (type.jvmErasure) {
            QueryResult::class -> value
            List::class -> value.list()
            Set::class -> value.set()
            structure.component -> if (type.isMarkedNullable) value.firstOrNull() else value.first()
            else -> error("Cannot produce $type as result")
        }
    }

    val SELECT_VALUE = annotationHandler<SelectValue> { type, function, args, annotation ->
        val (limit, offset, order, condition) = queryWindow(function, args)

        val target = if (annotation.raw) unsafe(annotation.value) else property<Any>(annotation.value)
        val queryType = when (type.jvmErasure) {
            QueryResult::class, List::class, Set::class -> type.arguments[0].type!!
            else -> type
        }

        val value = selectValue(target, queryType, where = createCondition(function, args) and condition, order = order, limit = limit, offset = offset)

        when (type.jvmErasure) {
            QueryResult::class -> value
            List::class -> value.list()
            Set::class -> value.set()
            queryType.jvmErasure -> if (queryType.isMarkedNullable) value.firstOrNull() else value.first()
            else -> error("Cannot produce $type as result")
        }
    }

    val INSERT = annotationHandler<Insert> { type, function, args, _ ->
        fun <T: Any> execute(table: TableImplementation<T>) = table.insert(createObject(table, function, args))
        val value = execute(this)

        when (type.jvmErasure) {
            Unit::class -> Unit
            UpdateResult::class -> value
            structure.component -> if (type.isMarkedNullable) value.value else value.getOrThrow()
            else -> error("Cannot produce $type as result")
        }
    }

    val UPSERT = annotationHandler<Insert> { type, function, args, _ ->
        fun <T: Any> execute(table: TableImplementation<T>) = table.upsert(createObject(table, function, args))
        val value = execute(this)

        when (type.jvmErasure) {
            Unit::class -> Unit
            UpdateResult::class -> value
            structure.component -> if (type.isMarkedNullable) value.value else value.getOrThrow()
            else -> error("Cannot produce $type as result")
        }
    }

    val UPDATE = annotationHandler<Update> { type, function, args, _ ->
        val updates = function.parameters
            .mapIndexed { index, value -> index to value }
            .filter { (_, it) -> it.isAnnotationPresent(Parameter::class.java) }
            .map { (index, param) ->
                property<Any>(param.getAnnotation(Parameter::class.java)!!.name.takeIf { it.isNotBlank() } ?: param.name) to
                value(args[index], function.kotlinFunction!!.valueParameters[index].type)
            }

        val value = update(columns = updates.toTypedArray(), where = createCondition(function, args))
        when (type.jvmErasure) {
            Unit::class -> Unit
            UpdateResult::class -> value
            Int::class -> if (type.isMarkedNullable) value.value else value.getOrThrow()
            Boolean::class -> (value.value ?: 0) > 0
            else -> error("Cannot produce $type as result")
        }
    }

    val DELETE = annotationHandler<Delete> { type, function, args, _ ->
        val value = delete(where = createCondition(function, args))
        when (type.jvmErasure) {
            Unit::class -> Unit
            Int::class -> value
            Boolean::class -> value > 0
            else -> error("Cannot produce $type as result")
        }
    }
}

private data class QueryWindow(val limit: Int?, val offset: Int?, val order: Order?, val condition: Where)
private fun queryWindow(function: Method, args: Array<out Any?>): QueryWindow {
    val limit = function.parameters
        .indexOfFirst { it.isAnnotationPresent(Limit::class.java) }
        .takeIf { it != -1 }
        ?.let { args[it] as Int }

    val offset = function.parameters
        .indexOfFirst { it.isAnnotationPresent(Offset::class.java) }
        .takeIf { it != -1 }
        ?.let { args[it] as Int }

    var order = null as Order?
    function.parameters
        .mapIndexed { index, it -> index to it }
        .filter { it.second.type == Order::class.java }
        .map { args[it.first] as Order }
        .forEach {
            if (order == null) order = it
            else order = order andThen it
        }

    @Suppress("UNCHECKED_CAST")
    val condition = allOf(function.parameters
        .mapIndexed { index, it -> index to it }
        .filter { it.second.type == Where::class.java }
        .map { args[it.first] as Where }
    )

    return QueryWindow(limit, offset, order, condition)
}