package setup

import de.mineking.database.AutoGenerate
import de.mineking.database.Column
import de.mineking.database.Key
import de.mineking.database.Unique

data class UserDao(
	@AutoGenerate @Key @Column val id: Int = 0,
	@Unique @Column val email: String = "",
	@Column val name: String = "",
	@Column val age: Int = 0
)