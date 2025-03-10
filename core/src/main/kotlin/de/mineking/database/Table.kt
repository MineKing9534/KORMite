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

	fun selectRowCount(where: Where = Where.EMPTY): Int
	fun <C> selectValue(target: Node<C>, type: KType, where: Where = Where.EMPTY, order: Order? = null, limit: Int? = null, offset: Int? = null): QueryResult<C>

	fun select(vararg columns: Node<*>, where: Where = Where.EMPTY, order: Order? = null, limit: Int? = null, offset: Int? = null): QueryResult<T>
	fun select(vararg columns: KProperty<*>, where: Where = Where.EMPTY, order: Order? = null, limit: Int? = null, offset: Int? = null): QueryResult<T> = select(columns = columns.map { property(it) }.toTypedArray(), where, order, limit, offset)
	fun select(where: Where = Where.EMPTY, order: Order? = null, limit: Int? = null, offset: Int? = null): QueryResult<T> = select(columns = emptyArray<KProperty<*>>(), where, order, limit, offset)

	fun update(obj: T): UpdateResult<T>
	fun update(vararg columns: Pair<Node<*>, Node<*>>, where: Where = Where.EMPTY): UpdateResult<Int>

	fun insert(obj: T): UpdateResult<T>
	fun upsert(obj: T): UpdateResult<T>

	fun delete(where: Where = Where.EMPTY): Int
	fun delete(obj: T) = delete(identifyObject(obj))
}

inline fun <reified T> DefaultTable<*>.selectValue(target: Node<T>, where: Where = Where.EMPTY, order: Order? = null, limit: Int? = null, offset: Int? = null): QueryResult<T> = selectValue(target, typeOf<T>(), where, order, limit, offset)

typealias ColumnContext = List<ColumnData<*, *>>
class ColumnData<O: Any, C>(
	val table: TableStructure<O>,
	val baseName: String,
	val name: String,
	val mapper: TypeMapper<C, *>,
	val property: KProperty1<O, C>,
	val key: Boolean,
	val autogenerate: Boolean
) {
	val meta = MetaData()

	val type get() = property.returnType

	fun get(obj: O): C = property.get(obj)
	fun set(obj: O, value: C?) = property.javaField?.apply { trySetAccessible() }?.set(obj, value) //Allow writing final
}

data class TableStructure<T: Any>(
	val manager: DatabaseConnection,
	val name: String,
	val namingStrategy: NamingStrategy,
	val columns: List<ColumnData<T, *>>,
	val component: KClass<T>
) {
	fun getColumnFromDatabase(name: String): ColumnData<T, *>? = columns.find { it.name == name }
	fun getColumnFromCode(name: String): ColumnData<T, *>? = columns.find { it.property.name == name }

	fun getKeys(): List<ColumnData<T, *>> = columns.filter { it.key }
}

data class MetaKey<T>(val name: String)
data class MetaData(private val data: MutableMap<String, Any?> = mutableMapOf()) {
	@Suppress("UNCHECKED_CAST")
	operator fun <T> get(key: MetaKey<T>) = data[key.name] as T?
	operator fun <T> set(key: MetaKey<T>, value: T) = data.put(key.name, value)
}


val REFERENCE_META_KEY = MetaKey<Table<*>>("reference")
var ColumnData<*, *>.reference: Table<*>?
	get() = meta[REFERENCE_META_KEY]
	set(value) {
		meta[REFERENCE_META_KEY] = value!!
	}