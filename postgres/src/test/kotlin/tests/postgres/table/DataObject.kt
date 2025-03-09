package tests.postgres.table

import de.mineking.database.*
import setup.ConsoleSqlLogger
import setup.createConnection
import setup.recreate
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

data class DataObjectReferenceDao(
	@AutoIncrement @Key @Column val id: Int = 0,
	@Column val reference: Int = 0
)

data class DataObjectDao(
	val main: DataObjectTest,
	@AutoIncrement @Key @Column val id: Int = 0,
	@Column val name: String = ""
) : DataObject<DataObjectDao> {
	override val table = main.table
}

class DataObjectTest {
	val connection = createConnection()
	val referenceTable = connection.getDefaultTable(name = "data_object_reference_test") { DataObjectReferenceDao() }
	val table = connection.getDefaultTable(name = "data_object_test") { DataObjectDao(this) }

	val references = arrayListOf<DataObjectReferenceDao>()

	init {
		table.recreate()
		referenceTable.recreate()

		val a = table.insert(DataObjectDao(this, name = "A")).value!!
		val b = table.insert(DataObjectDao(this, name = "B")).value!!

		references += referenceTable.insert(DataObjectReferenceDao(reference = a.id)).value!!
		references += referenceTable.insert(DataObjectReferenceDao(reference = b.id)).value!!
		references += referenceTable.insert(DataObjectReferenceDao(reference = b.id)).value!!
		references += referenceTable.insert(DataObjectReferenceDao(reference = a.id)).value!!
		references += referenceTable.insert(DataObjectReferenceDao(reference = b.id)).value!!

		connection.driver.setSqlLogger(ConsoleSqlLogger)
	}

	@Test
	fun selectAll() {
		val result = table.select().list()

		assertEquals(2, result.size)
		assertEquals(2, result[0].selectReferring(referenceTable, DataObjectReferenceDao::reference).list().size)
		assertEquals(3, result[1].selectReferring(referenceTable, DataObjectReferenceDao::reference).list().size)
	}

	@Test
	fun update() {
		assertTrue(referenceTable.update(references[1].copy(reference = 1)).isSuccess())

		assertEquals(3, table.select(where = property(DataObjectDao::name) isEqualTo value("A")).first().selectReferring(referenceTable, DataObjectReferenceDao::reference).list().size)
	}
}