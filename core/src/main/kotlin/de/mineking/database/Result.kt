package de.mineking.database

import org.jdbi.v3.core.result.ResultIterable
import java.sql.ResultSet
import java.sql.SQLException
import java.util.stream.Stream

data class ReadContext(val instance: Any?, val table: TableStructure<*>, val set: ResultSet, val selected: List<String>?, val prefix: Array<String> = emptyArray(), val autofillPrefix: (String) -> Boolean = { true }, var shouldRead: Boolean = true) {
    fun formatName(name: String) = ((prefix.takeIf { it.isNotEmpty() || !autofillPrefix(name) } ?: arrayOf(table.name)) + name).joinToString(".")

    fun <T> read(name: String, reader: (ResultSet, String) -> T): T = reader(set, formatName(name))
    fun shouldRead(name: String) = selected == null || formatName(name) in selected

    fun nest(name: String, table: TableImplementation<*>) = copy(instance = table.instance(), table = table.structure, prefix = prefix + name)
}

interface QueryResult<T> {
    fun list(): List<T>

    fun first(): T
    fun findFirst(): T? {
        return try { first() }
        catch (_: NoSuchElementException) { null }
    }

    fun stream(): Stream<T>
    fun <R> withStream(handler: (Stream<T>) -> R): R = stream().use(handler)
}

interface SimpleQueryResult<T>: QueryResult<T> {
    fun <O> execute(handler: (ResultIterable<T>) -> O): O

    override fun list(): List<T> = execute { it.list() }
    override fun first(): T = execute { it.first() }

    /**
     * Note: The returned stream has to be closed by the user!
     */
    override fun stream(): Stream<T> = execute { it.stream() }
}

data class UpdateResult<T>(
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
}