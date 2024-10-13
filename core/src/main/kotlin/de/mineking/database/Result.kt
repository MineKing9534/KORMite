package de.mineking.database

import org.jdbi.v3.core.result.ResultIterable
import java.sql.ResultSet
import java.sql.SQLException

data class ReadContext(val instance: Any?, val table: TableStructure<*>, val set: ResultSet, val selected: List<String>?, val prefix: Array<String> = emptyArray(), val autofillPrefix: (String) -> Boolean = { true }, var shouldRead: Boolean = true) {
    fun proceed(): Boolean {
        if (!shouldRead) return true

        shouldRead = false
        return set.next()
    }

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
}

interface RowQueryResult<T: Any> : QueryResult<T> {
    val instance: () -> T
    fun <O> execute(handler: ((T) -> Boolean) -> O): O

    override fun list(): List<T> = execute { read ->
        val result = arrayListOf<T>()

        while (true) {
            val obj = instance()

            if (!read(obj)) break
            result.add(obj)
        }

        result
    }

    override fun first(): T = execute { read ->
        val obj = instance()

        if (!read(obj)) throw NoSuchElementException()
        else obj
    }
}

interface ValueQueryResult<T>: QueryResult<T> {
    fun <O> execute(handler: (ResultIterable<T>) -> O): O

    override fun list(): List<T> = execute { it.list() }
    override fun first(): T = execute { it.first() }
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