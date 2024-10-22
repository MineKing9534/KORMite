package de.mineking.database.vendors.postgres

import de.mineking.database.DatabaseConnection
import de.mineking.database.NamingStrategy
import de.mineking.database.TableStructure
import de.mineking.database.TypeMapper
import mu.KotlinLogging
import org.jdbi.v3.core.Jdbi
import kotlin.reflect.KClass
import kotlin.reflect.full.memberProperties

class PostgresConnection(
	host: String,
	user: String,
	password: String,
	defaultNamingStrategy: NamingStrategy = NamingStrategy.Companion.DEFAULT
) : DatabaseConnection(Jdbi.create("jdbc:postgresql://$host", user, password), defaultNamingStrategy) {
	companion object {
		val logger = KotlinLogging.logger {}
	}

	init {
		typeMappers += PostgresMappers::class.memberProperties.map { it.get(PostgresMappers) as TypeMapper<*, *> }
	}

	override fun <T: Any> createTableImplementation(type: KClass<*>, structure: TableStructure<T>, instance: () -> T) = PostgresTable(type, structure, instance)
}