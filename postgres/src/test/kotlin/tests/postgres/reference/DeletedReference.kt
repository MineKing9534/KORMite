package tests.postgres.reference

import de.mineking.database.AutoIncrement
import de.mineking.database.Column
import de.mineking.database.Key
import de.mineking.database.Reference
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import setup.ConsoleSqlLogger
import setup.UserDao
import setup.createConnection
import setup.recreate
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals

data class DeletedReferenceDao(
	@AutoIncrement @Key @Column val id: Int = 0,
	@Reference("basic_test") @Column val user: UserDao = UserDao(),
	@Reference("basic_test") @Column val users: List<UserDao?> = emptyList()
)

class DeletedReferenceTest {
	val connection = createConnection()
	val userTable = connection.getDefaultTable(name = "basic_test") { UserDao() }
	val referenceTable = connection.getDefaultTable(name = "deleted_reference_test") { DeletedReferenceDao() }

	val users = listOf(
		UserDao(name = "Tom", email = "tom@example.com", age = 12),
		UserDao(name = "Alex", email = "alex@example.com", age = 23),
		UserDao(name = "Bob", email = "bob@example.com", age = 50),
		UserDao(name = "Eve", email = "eve@example.com", age = 42),
		UserDao(name = "Max", email = "max@example.com", age = 20)
	)

	init {
		userTable.recreate()
		referenceTable.recreate()

		users.forEach { userTable.insert(it) }

		referenceTable.insert(DeletedReferenceDao(user = users[0], users = users.reversed() + null))

		connection.driver.setSqlLogger(ConsoleSqlLogger)
	}

	@BeforeEach
	fun delete() {
		userTable.delete(users[0])
		userTable.delete(users[1])
	}

	@Test
	fun select() {
		val result = referenceTable.select().first()
		assertEquals(null, result.user as UserDao?)
		assertContentEquals(listOf("Max", "Eve", "Bob", null, null, null), result.users.map { it?.name })
	}
}