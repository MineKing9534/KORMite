package de.mineking.database.vendors

import de.mineking.database.*
import java.sql.ResultSet
import kotlin.reflect.KProperty
import kotlin.reflect.KType
import kotlin.reflect.jvm.jvmErasure

object SQLiteMappers {
	val ANY = typeMapper<Any>(DataType.VALUE, { set, name -> error("No suitable TypeMapper for insertion found") }, { value, statement, pos -> statement.setObject(pos, value) })

	val INTEGER = typeMapper<Int?>(SQLiteType.INTEGER, ResultSet::getInt)
	val BOOLEAN = typeMapper<Boolean?>(SQLiteType.INTEGER, { set, name -> set.getInt(name) > 0 })

	val STRING = typeMapper<String?>(SQLiteType.TEXT, ResultSet::getString)
	val ENUM = object : TypeMapper<Enum<*>?, String?> {
		override fun accepts(manager: DatabaseConnection, property: KProperty<*>?, type: KType): Boolean = type.jvmErasure.java.isEnum
		override fun getType(column: ColumnData<*, *>?, table: TableStructure<*>, property: KProperty<*>?, type: KType): DataType = SQLiteType.TEXT

		override fun format(column: ColumnData<*, *>?, table: TableStructure<*>, type: KType, value: Enum<*>?): String? = value?.name

		override fun extract(column: DirectColumnData<*, *>?, type: KType, context: ReadContext, name: String): String? = STRING.extract(column, type, context, name)
		override fun parse(column: DirectColumnData<*, *>?, type: KType, value: String?, context: ReadContext, name: String): Enum<*>? = value?.let { name -> type.jvmErasure.java.enumConstants.map { it as Enum<*> }.filter { it.name == name }.first() }
	}

	val BLOB = typeMapper<ByteArray?>(SQLiteType.BLOB, { set, name -> set.getBytes(name) })
}

enum class SQLiteType(override val sqlName: String) : DataType {
	INTEGER("integer"),
	REAL("real"),
	TEXT("text"),
	BLOB("blob");

	override fun toString(): String = sqlName
}