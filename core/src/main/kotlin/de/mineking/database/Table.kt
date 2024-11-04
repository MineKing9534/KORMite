package de.mineking.database

import org.jdbi.v3.core.kotlin.useHandleUnchecked
import java.lang.reflect.InvocationHandler
import java.lang.reflect.InvocationTargetException
import java.lang.reflect.Method
import kotlin.reflect.*
import kotlin.reflect.jvm.javaField

@Target(AnnotationTarget.FIELD)
@Retention(AnnotationRetention.RUNTIME)
annotation class Column(val name: String = "")

@Target(AnnotationTarget.FIELD)
@Retention(AnnotationRetention.RUNTIME)
annotation class AutoIncrement

@Target(AnnotationTarget.FIELD)
@Retention(AnnotationRetention.RUNTIME)
annotation class AutoGenerate(val generator: String = "")

@Target(AnnotationTarget.FIELD)
@Retention(AnnotationRetention.RUNTIME)
annotation class Key

@Target(AnnotationTarget.FIELD)
@Retention(AnnotationRetention.RUNTIME)
annotation class Unique(val name: String = "")

@Target(AnnotationTarget.FIELD)
@Retention(AnnotationRetention.RUNTIME)
annotation class Reference(val table: String)

@Target(AnnotationTarget.FIELD)
@Retention(AnnotationRetention.RUNTIME)
annotation class Json(val binary: Boolean = false)

interface Table<T: Any> {
	val structure: TableStructure<T>
	val implementation: TableImplementation<T>

	fun identifyObject(obj: T) = identifyObject(structure, obj)

	fun createTable()
	fun dropTable() = structure.manager.driver.useHandleUnchecked { it.createUpdate("drop table ${ structure.name }").execute() }

	fun selectRowCount(where: Where = Where.EMPTY): Int
	fun <C> selectValue(target: Node<C>, type: KType, where: Where = Where.EMPTY, order: Order? = null, limit: Int? = null, offset: Int? = null): QueryResult<C>

	fun select(vararg columns: Node<*>, where: Where = Where.EMPTY, order: Order? = null, limit: Int? = null, offset: Int? = null): QueryResult<T>
	fun select(vararg columns: KProperty<*>, where: Where = Where.EMPTY, order: Order? = null, limit: Int? = null, offset: Int? = null): QueryResult<T> = select(columns = columns.map { property(it) }.toTypedArray(), where, order, limit, offset)
	fun select(where: Where = Where.EMPTY, order: Order? = null, limit: Int? = null, offset: Int? = null): QueryResult<T> = select(columns = emptyArray<KProperty<*>>(), where, order, limit, offset)

	fun update(obj: T): UpdateResult<T>
	fun <T> update(column: Node<T>, value: Node<T>, where: Where = Where.EMPTY): UpdateResult<Int>
	fun <T> update(column: KProperty<T>, value: Node<T>, where: Where = Where.EMPTY): UpdateResult<Int> = update(property(column), value, where)

	fun insert(obj: T): UpdateResult<T>
	fun upsert(obj: T): UpdateResult<T>

	fun delete(where: Where = Where.EMPTY): Int
	fun delete(obj: T) = delete(identifyObject(obj))
}

inline fun <reified T> Table<*>.selectValue(target: Node<T>, where: Where = Where.EMPTY, order: Order? = null, limit: Int? = null, offset: Int? = null): QueryResult<T> = selectValue(target, typeOf<T>(), where, order, limit, offset)

abstract class TableImplementation<T: Any>(
	val type: KClass<*>,
	override val structure: TableStructure<T>,
	val instance: () -> T
) : Table<T>, InvocationHandler {
	override val implementation: TableImplementation<T> = this

	open fun parseResult(context: ReadContext) {
		@Suppress("UNCHECKED_CAST")
		val instance = context.instance as T

		fun <C> setField(column: DirectColumnData<T, C>) = column.set(instance, column.mapper.read(column, column.type, context, column.name))
		structure.columns.forEach { if (context.shouldRead(it.name)) setField(it) }

		if (context.instance is DataObject<*>) context.instance.afterRead()
	}

	override fun invoke(proxy: Any?, method: Method?, args: Array<out Any?>?): Any? {
		return try {
			javaClass.getMethod(method!!.name, *method.parameterTypes).invoke(this, *(args ?: emptyArray()))
		} catch(_: NoSuchMethodException) {
			type.java.classes.find { it.simpleName == "DefaultImpls" }?.getMethod(method!!.name, type.java, *method.parameterTypes)?.invoke(null, proxy, *(args ?: emptyArray()))
		} catch(e: IllegalAccessException) {
			throw RuntimeException(e)
		} catch(e: InvocationTargetException) {
			throw e.cause!!
		}
	}
}

sealed class ColumnData<O: Any, C>(
	val table: TableStructure<O>,
	val baseName: String,
	val name: String,
	val mapper: TypeMapper<C, *>,
	val type: KType
) {
	abstract fun get(obj: O): C
	abstract fun getRootColumn(): DirectColumnData<O, *>

	open fun shouldCreate(): Boolean = true
}

abstract class VirtualColumnData<O: Any, C> internal constructor(
	table: TableStructure<O>,
	baseName: String,
	name: String,
	val simpleName: String,
	mapper: TypeMapper<C, *>,
	type: KType,
	val parent: DirectColumnData<O, *>,
	val transform: ((String) -> String)?
) : ColumnData<O, C>(table, baseName, name, mapper, type) {
	override fun getRootColumn(): DirectColumnData<O, *> = parent
}

class DirectColumnData<O: Any, C> internal constructor(
	table: TableStructure<O>,
	baseName: String,
	name: String,
	mapper: TypeMapper<C, *>,
	val property: KProperty1<O, C>,
	val key: Boolean,
	val autogenerate: Boolean
) : ColumnData<O, C>(table, baseName, name, mapper, property.returnType) {
	var reference: Table<*>? = null
	private val children: MutableList<VirtualColumnData<O, *>> = arrayListOf()

	override fun get(obj: O): C = property.get(obj)
	fun set(obj: O, value: C?) = property.javaField?.apply { trySetAccessible() }?.set(obj, value) //Allow writing final

	fun getChildren(): List<VirtualColumnData<O, *>> = children

	override fun getRootColumn(): DirectColumnData<O, *> = this
	fun <T> createVirtualChild(baseName: String, name: String, simpleName: String, mapper: TypeMapper<T, *>, type: KType, isReference: Boolean = false, transform: ((String) -> String)? = null, value: (O) -> T): VirtualColumnData<O, T> {
		val column = object : VirtualColumnData<O, T>(table, baseName, name, simpleName, mapper, type, this, transform) {
			override fun get(obj: O): T = value(obj)
			override fun shouldCreate(): Boolean = !isReference
		}

		children.add(column)
		return column
	}
}

data class TableStructure<T: Any>(
	val manager: DatabaseConnection,
	val name: String,
	val namingStrategy: NamingStrategy,
	val columns: List<DirectColumnData<T, *>>
) {
	fun getColumnFromDatabase(name: String): ColumnData<T, *>? = getAllColumns().find { it.name == name }
	fun getColumnFromCode(name: String): DirectColumnData<T, *>? = columns.find { it.property.name == name }

	fun getAllColumns(): List<ColumnData<T, *>> {
		val result = arrayListOf<ColumnData<T, *>>()

		result += columns
		result += columns.flatMap { it.getChildren() }.filter { result.none { test -> test.name == it.name } }

		return result.filter { it.shouldCreate() }
	}

	fun getKeys(): List<ColumnData<T, *>> = getAllColumns().filter { it.getRootColumn().key }.filter { it.shouldCreate() }
}