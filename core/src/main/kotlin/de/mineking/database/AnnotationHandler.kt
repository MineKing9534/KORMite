package de.mineking.database

import java.lang.reflect.Method
import kotlin.reflect.KType
import kotlin.reflect.jvm.jvmErasure
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
        .map { (index, param) -> property<Any>(param.getAnnotation(Condition::class.java)!!.name.takeIf { it.isNotBlank() } ?: param.name) + " ${ param.getAnnotation(Condition::class.java)!!.operation } " + value(args[index]) }
        .map { Where(it) }
    )

    fun <T: Any> createObject(table: TableImplementation<T>, method: Method, args: Array<out Any?>): T {
        val obj = table.instance()

        method.parameters
            .mapIndexed { index, value -> index to value }
            .filter { (_, it) -> it.isAnnotationPresent(Parameter::class.java) }
            .forEach { (index, param) ->
                val name = param.getAnnotation(Parameter::class.java)!!.name.takeIf { it.isNotBlank() } ?: param.name

                @Suppress("UNCHECKED_CAST")
                val column = table.structure.getColumnFromCode(name) as DirectColumnData<Any, Any>? ?: error("Column $name not found")
                val value = args[index]

                column.set(obj, value)
            }

        return obj
    }

    val SELECT = annotationHandler<Select> { type, function, args, _ ->
        val value = select(where = createCondition(function, args))
        when (type.jvmErasure.java) {
            QueryResult::class.java -> value
            List::class.java -> value.list()
            structure.type.java -> if (type.isMarkedNullable) value.findFirst() else value.first()
            else -> error("Cannot produce $type as result")
        }
    }

    val INSERT = annotationHandler<Insert> { type, function, args, _ ->
        fun <T: Any> execute(table: TableImplementation<T>) = table.insert(createObject(table, function, args))
        val value = execute(this)

        when (type.jvmErasure.java) {
            UpdateResult::class.java -> value
            structure.type.java -> if (type.isMarkedNullable) value.value else value.getOrThrow()
            else -> error("Cannot produce $type as result")
        }
    }

    val UPSERT = annotationHandler<Insert> { type, function, args, _ ->
        fun <T: Any> execute(table: TableImplementation<T>) = table.upsert(createObject(table, function, args))
        val value = execute(this)

        when (type.jvmErasure.java) {
            UpdateResult::class.java -> value
            structure.type.java -> if (type.isMarkedNullable) value.value else value.getOrThrow()
            else -> error("Cannot produce $type as result")
        }
    }

    val UPDATE = annotationHandler<Update> { type, function, args, _ ->
        val updates = function.parameters
            .mapIndexed { index, value -> index to value }
            .filter { (_, it) -> it.isAnnotationPresent(Parameter::class.java) }
            .map { (index, param) -> property<Any>(param.getAnnotation(Parameter::class.java)!!.name.takeIf { it.isNotBlank() } ?: param.name) to value(args[index]) }

        val value = update(columns = updates.toTypedArray(), where = createCondition(function, args))
        when (type.jvmErasure.java) {
            UpdateResult::class.java -> value
            Int::class.java -> if (type.isMarkedNullable) value.value else value.getOrThrow()
            Boolean::class.java -> (if (type.isMarkedNullable) value.value ?: 0 else value.getOrThrow()) > 0
            else -> error("Cannot produce $type as result")
        }
    }

    val DELETE = annotationHandler<Delete> { type, function, args, _ ->
        val value = delete(where = createCondition(function, args))
        when (type.jvmErasure.java) {
            Int::class.java -> value
            Boolean::class.java -> value > 0
            else -> error("Cannot produce $type as result")
        }
    }
}