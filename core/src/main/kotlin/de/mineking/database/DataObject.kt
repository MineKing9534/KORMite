package de.mineking.database

import kotlin.reflect.KProperty

@Suppress("UNCHECKED_CAST")
interface DataObject<T: Any> {
	@Suppress("UNCHECKED_CAST")
	val table: Table<T> get() = sourceTable as Table<T>

	private val `this` get() = this as T

	fun insert() = table.insert(`this`)
	fun update() = table.update(`this`)
	fun upsert() = table.upsert(`this`)
	fun delete() = table.delete(`this`)

	fun <O: Any> selectReferring(table: Table<O>, reference: Node<*>, where: Where = Where.EMPTY): QueryResult<O> {
		val keys = this.table.structure.getKeys()
		require(keys.size == 1) { "Cannot select referring objects when having multiple keys" }

		return table.select(where = reference isEqualTo value(keys[0].get(`this`)) and where)
	}

	fun <O: Any> selectReferring(table: Table<O>, reference: KProperty<*>, where: Where = Where.EMPTY): QueryResult<O> = selectReferring(table, property(reference), where)

	fun beforeWrite() {}
	fun afterRead() {}
}