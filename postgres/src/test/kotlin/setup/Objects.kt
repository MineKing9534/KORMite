package setup

import de.mineking.database.AutoIncrement
import de.mineking.database.Column
import de.mineking.database.Key
import de.mineking.database.Unique

interface Identifiable {
	val id: Int
}

data class User(
	@AutoIncrement @Key @Column override val id: Int = 0,
	@Unique @Column val email: String = "",
	@Column val name: String = "",
	@Column val age: Int = 0
) : Identifiable