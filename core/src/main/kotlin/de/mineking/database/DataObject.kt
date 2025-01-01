package de.mineking.database

import kotlin.reflect.KProperty

@Suppress("UNCHECKED_CAST")
interface DataObject<T: Any> {
	val table: Table<T>

	fun insert() = table.insert(this as T)
	fun update() = table.update(this as T)
	fun upsert() = table.upsert(this as T)
	fun delete() = table.delete(this as T)

	fun <O: Any> selectReferring(table: Table<O>, reference: Node<*>, where: Where = Where.EMPTY): QueryResult<O> {
		val keys = this.table.structure.getKeys()
		require(keys.size == 1) { "Cannot select referring objects when having multiple keys" }

		return table.select(where = reference isEqualTo value(keys[0].get(this as T)) and where)
	}

	fun <O: Any> selectReferring(table: Table<O>, reference: KProperty<*>, where: Where = Where.EMPTY): QueryResult<O> = selectReferring(table, property(reference), where)

	fun beforeWrite() {}
	fun afterRead() {}
}