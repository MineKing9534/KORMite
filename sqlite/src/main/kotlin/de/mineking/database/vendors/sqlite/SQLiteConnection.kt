package de.mineking.database.vendors.sqlite

import de.mineking.database.*
import mu.KotlinLogging
import org.jdbi.v3.core.Jdbi
import kotlin.reflect.KClass
import kotlin.reflect.full.memberProperties

class SQLiteConnection(
	file: String,
	defaultNamingStrategy: NamingStrategy = NamingStrategy.DEFAULT
) : DatabaseConnection(Jdbi.create("jdbc:sqlite:$file"), defaultNamingStrategy) {
	companion object {
		val logger = KotlinLogging.logger {}
	}

	init {
		typeMappers += SQLiteMappers::class.memberProperties.map { it.get(SQLiteMappers) as TypeMapper<*, *> }
	}

	override fun <T: Any> createTableImplementation(tableType: KClass<*>, structure: TableStructure<T>, instance: () -> T): TableImplementation<T> = SQLiteTable(tableType, structure, instance)
}