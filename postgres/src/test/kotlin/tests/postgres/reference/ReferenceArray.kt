package tests.postgres.reference

import de.mineking.database.*
import de.mineking.database.vendors.postgres.contains
import org.junit.jupiter.api.Test
import setup.ConsoleSqlLogger
import setup.createConnection
import setup.recreate
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals

data class ReferenceArrayDao(
	@AutoIncrement @Key @Column val id: Int = 0,
	@Reference("book_test") @Column val books: List<BookDao?> = emptyList(),
	@Reference("book_test") @Column val book_array: Array<out BookDao?> = emptyArray()
)

class ReferenceArrayTest {
	val connection = createConnection()
	val publisherTable = connection.getDefaultTable(name = "publisher_test") { PublisherDao() }
	val authorTable = connection.getDefaultTable(name = "author_test") { AuthorDao() }
	val bookTable = connection.getDefaultTable(name = "book_test") { BookDao() }
	val referenceTable = connection.getDefaultTable(name = "reference_array_test") { ReferenceArrayDao() }

	val publisherA = PublisherDao(name = "A")
	val publisherB = PublisherDao(name = "B")

	val shakespeare = AuthorDao(name = "William Shakespeare", publisher = publisherA)
	val tolkien = AuthorDao(name = "J.R.R. Tolkien", publisher = publisherB)

	val hamlet = BookDao(title = "Hamlet", year = 1601, author = shakespeare, publisher = null)
	val romeoAndJuliet = BookDao(title = "Romeo and Juliet", year = 1595, author = shakespeare, publisher = publisherA)

	val hobbit = BookDao(title = "The Hobbit", year = 1937, author = tolkien, publisher = publisherA)
	val lotr = BookDao(title = "The Lord of the Rings", year = 1949, author = tolkien, publisher = publisherB)
	val silmarillion = BookDao(title = "Silmarillion", year = 1977, author = tolkien, publisher = publisherB)

	val firstTestList = arrayOf(hamlet, romeoAndJuliet, null, hamlet)
	val secondTestList = arrayOf(hobbit, lotr, silmarillion, lotr, hobbit, hamlet)

	init {
		publisherTable.recreate()
		authorTable.recreate()
		bookTable.recreate()
		referenceTable.recreate()

		publisherTable.insert(publisherA)
		publisherTable.insert(publisherB)

		authorTable.insert(shakespeare)
		authorTable.insert(tolkien)

		bookTable.insert(hamlet)
		bookTable.insert(romeoAndJuliet)

		bookTable.insert(hobbit)
		bookTable.insert(lotr)
		bookTable.insert(silmarillion)

		referenceTable.insert(ReferenceArrayDao(books = firstTestList.toList(), book_array = firstTestList))
		referenceTable.insert(ReferenceArrayDao(books = secondTestList.toList(), book_array = secondTestList))

		connection.driver.setSqlLogger(ConsoleSqlLogger)
	}

	@Test
	fun selectAll() {
		val result = referenceTable.select().list()

		assertContentEquals(firstTestList.toList(), result[0].books)
		assertContentEquals(firstTestList.toList(), result[0].book_array.toList())

		assertContentEquals(secondTestList.toList(), result[1].books)
		assertContentEquals(secondTestList.toList(), result[1].book_array.toList())
	}

	@Test
	fun selectContains() {
		assertEquals(1, referenceTable.selectRowCount(where = property(ReferenceArrayDao::books) contains value(romeoAndJuliet)))
		assertEquals(2, referenceTable.selectRowCount(where = property(ReferenceArrayDao::books) contains value(hamlet)))
	}
}