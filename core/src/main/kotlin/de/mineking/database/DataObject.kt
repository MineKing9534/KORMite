package de.mineking.database

import kotlin.reflect.KProperty

interface DataObject {
	fun beforeWrite() {}
	fun afterRead() {}
}

interface DefaultDataObject<T : Any> : DataObject {
	val table: DefaultTable<T>

	@Suppress("UNCHECKED_CAST")
	private val `this` get() = this as T

	fun insert() = table.insert(`this`)
	fun update() = table.update(`this`)
	fun upsert() = table.upsert(`this`)
	fun delete() = table.delete(`this`)

	fun <O: Any> selectReferring(table: DefaultTable<O>, reference: Node<*>, where: Where = Conditions.EMPTY): QueryResult<O> {
		val keys = this.table.structure.getKeys()
		require(keys.size == 1) { "Cannot select referring objects when having multiple keys" }

		return table.select(where = reference isEqualTo value(keys[0].get(`this`)) and where)
	}

	fun <O: Any> selectReferring(table: DefaultTable<O>, reference: KProperty<*>, where: Where = Conditions.EMPTY): QueryResult<O> = selectReferring(table, property(reference), where)
}