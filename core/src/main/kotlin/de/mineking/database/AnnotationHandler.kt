package de.mineking.database

import java.lang.reflect.Method
import kotlin.reflect.KType
import kotlin.reflect.KTypeProjection
import kotlin.reflect.full.*
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
        .map { Where(it) }
    )

    fun <T: Any> createObject(table: TableImplementation<T>, method: Method, args: Array<out Any?>): T {
        val obj = table.instance()

        method.kotlinFunction!!.valueParameters
            .mapIndexed { index, value -> index to value }
            .filter { (_, it) -> it.hasAnnotation<Parameter>() }
            .forEach { (index, param) ->
                val name = param.findAnnotation<Parameter>()!!.name.takeIf { it.isNotBlank() } ?: param.name!!

                @Suppress("UNCHECKED_CAST")
                val column = table.structure.getColumnFromCode(name) as DirectColumnData<Any, Any?>? ?: error("Column $name not found")
                val value = args[index]

                try {
                    //Try direct
                    column.set(obj, value)
                } catch(_: IllegalArgumentException) {
                    @Suppress("UNCHECKED_CAST")
                    //Try parsing with column mapper
                    column.set(obj, (column.mapper as TypeMapper<*, Any?>).parse(column, param.type, value, ReadContext(obj, table.structure, createDummy(), emptyList()), ""))
                } catch(_: IllegalArgumentException) {
                    //Try formatting with value mapper
                    column.set(obj, table.structure.manager.getTypeMapper<Any?, Any>(param.type, null)!!.format(column, table.structure, param.type, value))
                }
            }

        return obj
    }

    val SELECT = annotationHandler<Select> { type, function, args, _ ->
        val (limit, offset, order, condition) = queryWindow(function, args)

        val value = select(where = createCondition(function, args) and condition, order = order, limit = limit, offset = offset)
        when (type.jvmErasure.java) {
            QueryResult::class.java -> value
            List::class.java -> value.list()
            Set::class.java -> value.list().toSet()
            structure.component.java -> if (type.isMarkedNullable) value.findFirst() else value.first()
            else -> error("Cannot produce $type as result")
        }
    }

    val SELECT_VALUE = annotationHandler<SelectValue> { type, function, args, annotation ->
        val (limit, offset, order, condition) = queryWindow(function, args)

        val valueType =
            if (annotation.type == Unit::class) structure.getColumnFromCode(annotation.value)?.type ?: error("Cannot find ${ annotation.value } as column")
            else annotation.type.createType(annotation.typeParameters.mapIndexed { i, _ -> KTypeProjection.invariant(annotation.typeParameters[i].createType()) })

        val value = selectValue(property<Any?>(annotation.value), valueType, where = createCondition(function, args) and condition, order = order, limit = limit, offset = offset)

        if (valueType.isSubtypeOf(type)) return@annotationHandler if (type.isMarkedNullable) value.findFirst() else value.first()

        when (type.jvmErasure.java) {
            QueryResult::class.java -> value
            List::class.java -> value.list()
            Set::class.java -> value.list().toSet()
            else -> error("Cannot produce $type as result")
        }
    }

    val INSERT = annotationHandler<Insert> { type, function, args, _ ->
        fun <T: Any> execute(table: TableImplementation<T>) = table.insert(createObject(table, function, args))
        val value = execute(this)

        when (type.jvmErasure.java) {
            Unit::class.java -> Unit
            UpdateResult::class.java -> value
            structure.component.java -> if (type.isMarkedNullable) value.value else value.getOrThrow()
            else -> error("Cannot produce $type as result")
        }
    }

    val UPSERT = annotationHandler<Insert> { type, function, args, _ ->
        fun <T: Any> execute(table: TableImplementation<T>) = table.upsert(createObject(table, function, args))
        val value = execute(this)

        when (type.jvmErasure.java) {
            Unit::class.java -> Unit
            UpdateResult::class.java -> value
            structure.component.java -> if (type.isMarkedNullable) value.value else value.getOrThrow()
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
        when (type.jvmErasure.java) {
            Unit::class.java -> Unit
            UpdateResult::class.java -> value
            Int::class.java -> if (type.isMarkedNullable) value.value else value.getOrThrow()
            Boolean::class.java -> (value.value ?: 0) > 0
            else -> error("Cannot produce $type as result")
        }
    }

    val DELETE = annotationHandler<Delete> { type, function, args, _ ->
        val value = delete(where = createCondition(function, args))
        when (type.jvmErasure.java) {
            Unit::class.java -> Unit
            Int::class.java -> value
            Boolean::class.java -> value > 0
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

    val condition = allOf(function.parameters
        .mapIndexed { index, it -> index to it }
        .filter { it.second.type == Where::class.java }
        .map { args[it.first] as Where }
    )

    return QueryWindow(limit, offset, order, condition)
}