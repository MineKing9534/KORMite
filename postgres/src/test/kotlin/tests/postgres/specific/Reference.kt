package tests.postgres.specific

import de.mineking.database.*
import org.junit.jupiter.api.Test
import setup.ConsoleSqlLogger
import setup.createConnection
import setup.recreate
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertNull

data class Publisher(
	@AutoIncrement @Key @Column val id: Int = 0,
	@Column val name: String = ""
)

data class Author(
	@AutoIncrement @Key @Column val id: Int = 0,
	@Column val name: String = "",
	@Reference("publisher_test") @Column val publisher: Publisher = Publisher()
)

data class Book(
	@AutoIncrement @Key @Column val id: Int = 0,
	@Column val title: String = "",
	@Column val year: Int = 0,
	@Reference(table = "author_test") @Column val author: Author = Author(),
	@Reference(table = "publisher_test") @Column val publisher: Publisher? = Publisher()
)

class ReferenceTest {
	val connection = createConnection()
	val publisherTable = connection.getDefaultTable(name = "publisher_test") { Publisher() }
	val authorTable = connection.getDefaultTable(name = "author_test") { Author() }
	val bookTable = connection.getDefaultTable(name = "book_test") { Book() }

	val publisherA = Publisher(name = "A")
	val publisherB = Publisher(name = "B")

	val shakespeare = Author(name = "William Shakespeare", publisher = publisherA)
	val tolkien = Author(name = "J.R.R. Tolkien", publisher = publisherB)

	val hamlet = Book(title = "Hamlet", year = 1601, author = shakespeare, publisher = null)
	val romeoAndJuliet = Book(title = "Romeo and Juliet", year = 1595, author = shakespeare, publisher = publisherA)

	val hobbit = Book(title = "The Hobbit", year = 1937, author = tolkien, publisher = publisherA)
	val lotr = Book(title = "The Lord of the Rings", year = 1949, author = tolkien, publisher = publisherB)
	val silmarillion = Book(title = "Silmarillion", year = 1977, author = tolkien, publisher = publisherB)

	init {
		publisherTable.recreate()
		authorTable.recreate()
		bookTable.recreate()

		publisherTable.insert(publisherA)
		publisherTable.insert(publisherB)

		authorTable.insert(shakespeare)
		authorTable.insert(tolkien)

		bookTable.insert(hamlet)
		bookTable.insert(romeoAndJuliet)

		bookTable.insert(hobbit)
		bookTable.insert(lotr)
		bookTable.insert(silmarillion)

		connection.driver.setSqlLogger(ConsoleSqlLogger)
	}

	@Test
	fun selectAll() {
		val result = bookTable.select().list()

		assertEquals(5, result.size)

		assertContentEquals(listOf(hamlet, romeoAndJuliet, hobbit, lotr, silmarillion), result)
	}

	@Test
	fun selectReferenceCondition() {
		assertEquals(2, bookTable.select(where = property(Book::author, Author::name) isEqualTo value("William Shakespeare")).list().size)
		assertEquals(3, bookTable.select(where = property(Book::author, Author::name) isEqualTo value("J.R.R. Tolkien")).list().size)

		assertEquals(2, bookTable.select(where = property(Book::author, Author::publisher, Publisher::name) isEqualTo value("A")).list().size)
		assertEquals(3, bookTable.select(where = property(Book::author, Author::publisher, Publisher::name) isEqualTo value("B")).list().size)
	}

	@Test
	fun selectValue() {
		val result = bookTable.selectValue(property(Book::title).uppercase(), where = property(Book::publisher) isNotEqualTo property(Book::author, Author::publisher)).list()

		assertEquals(1, result.size)
		assertEquals("THE HOBBIT", result.first())
	}

	@Test
	fun selectReference() {
		assertEquals(tolkien, bookTable.selectValue(property(Book::author), where = property(Book::title) isEqualTo value("The Hobbit")).first())
		assertEquals(tolkien.id, bookTable.selectValue(property<Int>(Book::author), where = property(Book::title) isEqualTo value("The Hobbit")).first())
	}

	@Test
	fun updateReference() {
		bookTable.update(property(Book::publisher) to value(publisherB), where = property(Book::title) isEqualTo value("The Hobbit"))
		assertEquals(publisherB, bookTable.selectValue(property(Book::publisher), where = property(Book::title) isEqualTo value("The Hobbit")).first())
	}

	@Test
	fun deletedReference() {
		authorTable.delete(tolkien)
		val result = bookTable.select(where = property(Book::author) isEqualTo value(tolkien)).list()

		assertEquals(3, result.size)
		result.forEach { assertNull(it.author) }
	}
}