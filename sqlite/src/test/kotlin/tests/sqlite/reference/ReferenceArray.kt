package tests.sqlite.reference

import de.mineking.database.AutoIncrement
import de.mineking.database.Column
import de.mineking.database.Key
import de.mineking.database.Reference
import de.mineking.database.vendors.SQLiteConnection
import org.junit.jupiter.api.Test
import setup.ConsoleSqlLogger
import setup.recreate
import kotlin.test.assertContentEquals

data class ReferenceArrayDao(
	@AutoIncrement @Key @Column val id: Int = 0,
	@Reference("book_test") @Column val books: List<BookDao?> = emptyList()
)

class ReferenceArrayTest {
	val connection = SQLiteConnection("test.db")
	val publisherTable = connection.getTable(name = "publisher_test") { PublisherDao() }
	val authorTable = connection.getTable(name = "author_test") { AuthorDao() }
	val bookTable = connection.getTable(name = "book_test") { BookDao() }
	val referenceTable = connection.getTable(name = "reference_array_test") { ReferenceArrayDao() }

	val publisherA = PublisherDao(name = "A")
	val publisherB = PublisherDao(name = "B")

	val shakespeare = AuthorDao(name = "William Shakespeare", publisher = publisherA)
	val tolkien = AuthorDao(name = "J.R.R. Tolkien", publisher = publisherB)

	val hamlet = BookDao(title = "Hamlet", year = 1601, author = shakespeare, publisher = null)
	val romeoAndJulia = BookDao(title = "Romeo and Julia", year = 1595, author = shakespeare, publisher = publisherA)

	val hobbit = BookDao(title = "The Hobbit", year = 1937, author = tolkien, publisher = publisherA)
	val lotr = BookDao(title = "The Lord of the Rings", year = 1949, author = tolkien, publisher = publisherB)
	val silmarillion = BookDao(title = "Silmarillion", year = 1977, author = tolkien, publisher = publisherB)

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
		bookTable.insert(romeoAndJulia)

		bookTable.insert(hobbit)
		bookTable.insert(lotr)
		bookTable.insert(silmarillion)

		referenceTable.insert(ReferenceArrayDao(books = listOf(hamlet, romeoAndJulia, null, hamlet)))
		referenceTable.insert(ReferenceArrayDao(books = listOf(hobbit, lotr, silmarillion, lotr, hobbit, hamlet)))

		connection.driver.setSqlLogger(ConsoleSqlLogger)
	}

	@Test
	fun selectAll() {
		val result = referenceTable.select().list()

		assertContentEquals(listOf(hamlet, romeoAndJulia, null, hamlet), result[0].books)
		assertContentEquals(listOf(hobbit, lotr, silmarillion, lotr, hobbit, hamlet), result[1].books)
	}
}