package tests.sqlite.reference

import de.mineking.database.*
import de.mineking.database.vendors.sqlite.SQLiteConnection
import org.junit.jupiter.api.Test
import setup.ConsoleSqlLogger
import setup.recreate
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals

data class PublisherDao(
	@AutoIncrement @Key @Column val id: Int = 0,
	@Column val name: String = ""
)

data class AuthorDao(
	@AutoIncrement @Key @Column val id: Int = 0,
	@Column val name: String = "",
	@Reference("publisher_test") @Column val publisher: PublisherDao = PublisherDao()
)

data class BookDao(
	@AutoIncrement @Key @Column val id: Int = 0,
	@Column val title: String = "",
	@Column val year: Int = 0,
	@Reference(table = "author_test") @Column val author: AuthorDao = AuthorDao(),
	@Reference(table = "publisher_test") @Column val publisher: PublisherDao? = PublisherDao()
)

class ReferenceTest {
	val connection = SQLiteConnection("test.db")
	val publisherTable = connection.getTable(name = "publisher_test") { PublisherDao() }
	val authorTable = connection.getTable(name = "author_test") { AuthorDao() }
	val bookTable = connection.getTable(name = "book_test") { BookDao() }

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

		publisherTable.insert(publisherA)
		publisherTable.insert(publisherB)

		authorTable.insert(shakespeare)
		authorTable.insert(tolkien)

		bookTable.insert(hamlet)
		bookTable.insert(romeoAndJulia)

		bookTable.insert(hobbit)
		bookTable.insert(lotr)
		bookTable.insert(silmarillion)

		connection.driver.setSqlLogger(ConsoleSqlLogger)
	}

	@Test
	fun selectAll() {
		val result = bookTable.select().list()

		assertEquals(5, result.size)

		assertContentEquals(listOf(hamlet, romeoAndJulia, hobbit, lotr, silmarillion), result)
	}

	@Test
	fun selectSingleReference() {
		assertEquals(2, bookTable.select(where = property("author->name") isEqualTo value("William Shakespeare")).list().size)
		assertEquals(3, bookTable.select(where = property("author->name") isEqualTo value("J.R.R. Tolkien")).list().size)
	}

	@Test
	fun selectDoubleReference() {
		assertEquals(2, bookTable.select(where = property("author->publisher->name") isEqualTo value("A")).list().size)
		assertEquals(3, bookTable.select(where = property("author->publisher->name") isEqualTo value("B")).list().size)
	}

	@Test
	fun selectSingle() {
		val result = bookTable.select<String>(upperCase(property("title")), where = property("publisher") isNotEqualTo property("author->publisher")).list()

		assertEquals(1, result.size)
		assertEquals("THE HOBBIT", result.first())
	}

	@Test
	fun selectReference() {
		assertEquals(tolkien, bookTable.select<AuthorDao>(property("author"), where = property("title") isEqualTo value("The Hobbit")).first())
	}

	@Test
	fun updateReference() {
		bookTable.update("publisher", value(publisherB), where = property("title") isEqualTo value("The Hobbit"))
		assertEquals(publisherB, bookTable.select<PublisherDao>(property("publisher"), where = property("title") isEqualTo value("The Hobbit")).first())
	}
}