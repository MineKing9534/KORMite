package de.mineking.database

import kotlin.reflect.*
import kotlin.reflect.jvm.javaField

interface Table<T: Any> {
	val structure: TableStructure<T>
	val implementation: TableImplementation<T>
}

fun <T> Table<*>.data(name: String) = structure.manager.data<T>(name)

interface DefaultTable<T: Any> : Table<T> {
	fun identifyObject(obj: T) = identifyObject(structure, obj)

	fun selectRowCount(where: Where = Conditions.EMPTY): Int
	fun <C> selectValue(target: Node<C>, type: KType, where: Where = Conditions.EMPTY, order: Order? = null, limit: Int? = null, offset: Int? = null): QueryResult<C>

	fun select(vararg columns: Node<*>, where: Where = Conditions.EMPTY, order: Order? = null, limit: Int? = null, offset: Int? = null): QueryResult<T>
	fun select(vararg columns: KProperty<*>, where: Where = Conditions.EMPTY, order: Order? = null, limit: Int? = null, offset: Int? = null): QueryResult<T> = select(columns = columns.map { property(it) }.toTypedArray(), where, order, limit, offset)
	fun select(where: Where = Conditions.EMPTY, order: Order? = null, limit: Int? = null, offset: Int? = null): QueryResult<T> = select(columns = emptyArray<KProperty<*>>(), where, order, limit, offset)

	fun update(obj: T): Result<T>
	fun update(vararg columns: Pair<Node<*>, Node<*>>, where: Where = Conditions.EMPTY): Result<Int>

	fun updateReturning(vararg columns: Pair<Node<*>, Node<*>>, where: Where = Conditions.EMPTY): ErrorHandledQueryResult<T> = implementation.updateReturning(*columns, where = where, returning = null, type = null)
	fun <C> updateReturning(vararg columns: Pair<Node<*>, Node<*>>, returning: Node<C>, type: KType, where: Where = Conditions.EMPTY): ErrorHandledQueryResult<C> = implementation.updateReturning(*columns, where = where, returning = returning as Node<*>?, type = type)

	fun insert(obj: T): Result<T>
	fun upsert(obj: T): Result<T>

	fun delete(where: Where = Conditions.EMPTY): Int
	fun delete(obj: T) = delete(identifyObject(obj))
}

inline fun <reified T> DefaultTable<*>.selectValue(target: Node<T>, where: Where = Conditions.EMPTY, order: Order? = null, limit: Int? = null, offset: Int? = null): QueryResult<T> = selectValue(target, typeOf<T>(), where, order, limit, offset)
inline fun <reified T> DefaultTable<*>.updateReturning(vararg columns: Pair<Node<*>, Node<*>>, returning: Node<T>, where: Where = Conditions.EMPTY): ErrorHandledQueryResult<T> = updateReturning(*columns, returning = returning, type = typeOf<T>(), where = where)

typealias ColumnContext = List<PropertyData<*, *>>
sealed class PropertyData<O: Any, C>(
	val table: TableStructure<O>,
	val mapper: TypeMapper<C, *>,
	val property: KProperty1<O, C>
) {
	val meta = MetaData()
	val type get() = property.returnType

	abstract val name: String

	fun get(obj: O): C = property.get(obj)
	fun set(obj: O, value: C?) = property.javaField?.apply { trySetAccessible() }?.set(obj, value) //Allow writing final

	abstract fun format(context: ColumnContext, prefix: Boolean): String
}

class ColumnData<O: Any, C>(
	table: TableStructure<O>,
	override val name: String,
	mapper: TypeMapper<C, *>,
	property: KProperty1<O, C>,
	val key: Boolean,
	val autogenerate: Boolean
) : PropertyData<O, C>(table, mapper, property) {
	override fun format(context: ColumnContext, prefix: Boolean) =
		if (prefix) "\"${ if (context.size == 1) table.name else context.dropLast(1).joinToString(".") { it.name } }\".\"$name\""
		else "\"$name\""
}

class SelectOnlyPropertyData<O: Any, C>(
	table: TableStructure<O>,
	mapper: TypeMapper<C, *>,
	property: KProperty1<O, C>,
	val sql: String
) : PropertyData<O, C>(table, mapper, property) {
	override val name = property.name
	override fun format(context: ColumnContext, prefix: Boolean) = sql
}

data class TableStructure<T: Any>(
	val manager: DatabaseConnection,
	val name: String,
	val namingStrategy: NamingStrategy,
	val properties: List<PropertyData<T, *>>,
	val component: KClass<T>
) {
	val columns get() = properties.filterIsInstance<ColumnData<T, *>>()

	fun getFromDatabase(name: String) = columns.find { it.name == name }
	fun getFromCode(name: String) = properties.find { it.property.name == name }

	fun getKeys() = columns.filter { it.key }
}

data class MetaKey<T>(val name: String)
data class MetaData(private val data: MutableMap<String, Any?> = mutableMapOf()) {
	@Suppress("UNCHECKED_CAST")
	operator fun <T> get(key: MetaKey<T>) = data[key.name] as T?
	operator fun <T> set(key: MetaKey<T>, value: T) = data.put(key.name, value)
}


val REFERENCE_META_KEY = MetaKey<Table<*>>("reference")
var PropertyData<*, *>.reference: Table<*>?
	get() = meta[REFERENCE_META_KEY]
	set(value) {
		meta[REFERENCE_META_KEY] = value!!
	}