package de.mineking.database

@Suppress("UNCHECKED_CAST")
interface DataObject<T: Any> {
	fun getTable(): Table<T>

	fun insert() = getTable().insert(this as T)
	fun update() = getTable().update(this as T)
	fun upsert() = getTable().upsert(this as T)
	fun delete() = getTable().delete(this as T)

	fun <O: Any> selectReferring(table: Table<O>, reference: String, where: Where = Where.EMPTY): QueryResult<O> {
		val keys = getTable().structure.getKeys()
		require(keys.size == 1) { "Cannot select referring objects when having multiple keys" }

		return table.select(where = property(reference) isEqualTo value(keys[0].get(this as T)) and where)
	}

	fun beforeWrite() {}
	fun afterRead() {}
}