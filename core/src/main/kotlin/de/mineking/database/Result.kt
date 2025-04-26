package de.mineking.database

import org.jdbi.v3.core.statement.StatementContext
import java.sql.ResultSet
import java.sql.SQLException
import java.util.*
import java.util.stream.Stream
import java.util.stream.StreamSupport
import kotlin.reflect.KType

class QueryIterator<T>(val context: ReadContext, val statement: StatementContext, val position: Int, val column: ColumnContext, val type: KType, val mapper: TypeMapper<T, *>) : Iterator<T>, AutoCloseable {
    private var closed = false

    private var hasNext = false
    private var moved = false

    override fun hasNext(): Boolean {
        if (closed) return false
        if (moved) return hasNext

        hasNext = next0()

        if (hasNext) moved = true
        else close()

        return hasNext
    }

    override fun next(): T {
        if (!hasNext()) {
            close()
            error("No new element available")
        }

        try {
            return mapper.read(column, type, context, position)
        } finally {
            moved = next0()
            if (!moved) close()
        }
    }

    override fun close() {
        closed = true
        statement.close()
    }

    private fun next0() = context.set.next()
}

data class ReadContext(val table: TableImplementation<*>, val set: ResultSet, val columns: List<ColumnContext>, val currentContext: ColumnContext = emptyList(), val instance: Any? = null) {
    fun <T> read(position: Int, extractor: (ResultSet, Int) -> T): T = extractor(set, position)
    fun nest(column: PropertyData<*, *>) = copy(currentContext = currentContext + column, instance = null)
}

interface QueryResult<T> {
    fun <R> useIterator(handler: (QueryIterator<T>) -> R): R

    fun <R> useStream(handler: (Stream<T>) -> R): R = useIterator { handler(StreamSupport.stream(Spliterators.spliteratorUnknownSize(it, 0), false).onClose(it::close)) }
    fun <R> useSequence(handler: (Sequence<T>) -> R): R = useIterator { handler(it.asSequence()) }

    fun list(): List<T> = useSequence { it.toList() }
    fun set(): Set<T> = useSequence { it.toSet() }

    fun first(): T = useIterator { it.next() }
    fun firstOrNull(): T? = useIterator { if (it.hasNext()) it.next() else null }
}

interface ErrorHandledQueryResult<T> {
    fun execute(): Result<Unit>
    fun <R> useIterator(handler: (QueryIterator<T>) -> R): Result<R>

    fun <R> useStream(handler: (Stream<T>) -> R): Result<R> = useIterator { handler(StreamSupport.stream(Spliterators.spliteratorUnknownSize(it, 0), false).onClose(it::close)) }
    fun <R> useSequence(handler: (Sequence<T>) -> R): Result<R> = useIterator { handler(it.asSequence()) }

    fun list(): Result<List<T>> = useSequence { it.toList() }
    fun set(): Result<Set<T>> = useSequence { it.toSet() }

    fun first(): Result<T> = useIterator { it.next() }
    fun firstOrNull(): Result<T?> = useIterator { if (it.hasNext()) it.next() else null }

    fun ignoreErrors() = object : QueryResult<T> {
        override fun <R> useIterator(handler: (QueryIterator<T>) -> R): R = this@ErrorHandledQueryResult.useIterator(handler).getOrThrow()
    }
}

data class Result<out T>(
    val value: T?,
    val error: SQLException?,

    val uniqueViolation: Boolean,
    val notNullViolation: Boolean
) {
    fun isError() = error != null
    fun isSuccess() = error == null

    @Suppress("UNCHECKED_CAST")
    fun getOrThrow(): T = when {
        error != null -> throw error
        else -> value as T
    }

    fun orNull(): T? = when {
        error != null -> null
        else -> value
    }
}

fun <T> Result<T>.orElse(other: T) = when {
    isSuccess() -> getOrThrow()
    else -> other
}