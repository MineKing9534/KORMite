package tests.sqlite.specific

import de.mineking.database.*
import org.junit.jupiter.api.Test
import setup.ConsoleSqlLogger
import setup.createConnection
import setup.recreate
import java.util.*
import kotlin.test.assertContains
import kotlin.test.assertEquals

enum class TestEnum { A, B, C }
data class EnumTestObject(
	@AutoIncrement @Key @Column val id: Int = 0,
	@Column val single: TestEnum = TestEnum.A,
	@Column val multi: EnumSet<TestEnum> = EnumSet.noneOf(TestEnum::class.java)
)

class EnumTest {
	val connection = createConnection()
	val table = connection.getDefaultTable(name = "enum_test") { EnumTestObject() }

	init {
		table.recreate()

		table.insert(EnumTestObject(single = TestEnum.A, multi = EnumSet.of(TestEnum.A, TestEnum.C)))

		connection.driver.setSqlLogger(ConsoleSqlLogger)
	}

	@Test
	fun selectAll() {
		val result = table.select().first()

		assertEquals(TestEnum.A, result.single)

		assertEquals(2, result.multi.size)
		assertContains(result.multi, TestEnum.A)
		assertContains(result.multi, TestEnum.C)
	}

	@Test
	fun selectSingle() {
		assertEquals(TestEnum.A, table.selectValue(property(EnumTestObject::single)).first())
	}
}