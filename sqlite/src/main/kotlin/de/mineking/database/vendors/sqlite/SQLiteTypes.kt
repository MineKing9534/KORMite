package de.mineking.database.vendors.sqlite

import com.google.gson.GsonBuilder
import com.google.gson.ToNumberStrategy
import de.mineking.database.*
import org.jdbi.v3.core.argument.Argument
import org.jdbi.v3.core.statement.StatementContext
import java.awt.Color
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import java.sql.*
import java.sql.Date
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.util.*
import kotlin.reflect.KProperty
import kotlin.reflect.KType
import kotlin.reflect.jvm.javaType
import kotlin.reflect.jvm.jvmErasure

object SQLiteMappers {
	val BOOLEAN = nullSafeTypeMapper<Boolean>(SQLiteType.INTEGER, { set, position -> set.getInt(position) > 0 }, { value, statement, position -> statement.setInt(position, if (value) 1 else 0) })
	val BYTE_ARRAy = typeMapper<ByteArray?>(SQLiteType.BLOB, ResultSet::getBytes)
	val BLOB = typeMapper<Blob?>(SQLiteType.BLOB, ResultSet::getBlob)

	val SHORT = nullSafeTypeMapper<Short>(SQLiteType.INTEGER, ResultSet::getShort)
	val INTEGER = nullSafeTypeMapper<Int>(SQLiteType.INTEGER, ResultSet::getInt)
	val LONG = nullSafeTypeMapper<Long>(SQLiteType.INTEGER, ResultSet::getLong)

	val FLOAT = nullSafeTypeMapper<Float>(SQLiteType.REAL, ResultSet::getFloat)
	val DOUBLE = nullSafeTypeMapper<Double>(SQLiteType.REAL, ResultSet::getDouble)

	val STRING = typeMapper<String?>(SQLiteType.TEXT, ResultSet::getString)
	val ENUM = object : TypeMapper<Enum<*>?, String?> {
		override fun accepts(manager: DatabaseConnection, property: KProperty<*>?, type: KType): Boolean = type.jvmErasure.java.isEnum

		override fun getType(column: ColumnData<*, *>?, table: TableStructure<*>, type: KType): DataType =
            SQLiteType.TEXT

		override fun format(column: ColumnContext, table: TableStructure<*>, type: KType, value: Enum<*>?): String? = value?.name

		override fun extract(column: ColumnContext, type: KType, context: ReadContext, position: Int): String? = STRING.extract(column, type, context, position)
		override fun parse(column: ColumnContext, type: KType, value: String?, context: ReadContext, position: Int): Enum<*>? = value?.let { name -> type.jvmErasure.java.enumConstants.map { it as Enum<*> }.first { it.name == name } }
	}

	val JSON = object : TypeMapper<Any?, String?> {
		val numberStrategy = ToNumberStrategy { reader ->
			val str = reader!!.nextString()
			if (str.contains(".")) str.toDouble() else str.toInt()
		}
		val gson = GsonBuilder()
			.setNumberToNumberStrategy(numberStrategy)
			.setObjectToNumberStrategy(numberStrategy)
			.create()

		override fun accepts(manager: DatabaseConnection, property: KProperty<*>?, type: KType): Boolean = property?.hasDatabaseAnnotation<Json>() == true
		override fun getType(column: ColumnData<*, *>?, table: TableStructure<*>, type: KType): DataType =
            SQLiteType.TEXT

		override fun format(column: ColumnContext, table: TableStructure<*>, type: KType, value: Any?): String? = value?.let { gson.toJson(value) }
		override fun createArgument(column: ColumnContext, table: TableStructure<*>, type: KType, value: String?): Argument = object : Argument {
			override fun apply(position: Int, statement: PreparedStatement?, ctx: StatementContext?) {
				if (value == null) statement?.setNull(position, Types.NULL)
				else statement?.setString(position, value)
			}

			override fun toString(): String = value.toString()
		}

		override fun extract(column: ColumnContext, type: KType, context: ReadContext, position: Int): String? = STRING.extract(column, type, context, position)
		override fun parse(column: ColumnContext, type: KType, value: String?, context: ReadContext, position: Int): Any? = value?.let { gson.fromJson(it, type.javaType) }
	}

	val INSTANT = typeMapper<Instant?>(SQLiteType.INTEGER, { set, position -> set.getTimestamp(position).toInstant() }, { value, statement, position -> statement.setTimestamp(position, value?.let { Timestamp.from(it) }) })
	val LOCAL_DATE_TIME = typeMapper<LocalDateTime?>(SQLiteType.INTEGER, { set, position -> set.getTimestamp(position).toLocalDateTime() }, { value, statement, position -> statement.setTimestamp(position, value?.let { Timestamp.valueOf(it) }) })
	val LOCAL_DATE = typeMapper<LocalDate?>(SQLiteType.INTEGER, { set, position -> set.getDate(position).toLocalDate() }, { value, statement, position -> statement.setDate(position, value?.let { Date.valueOf(it) }) })

	val LOCALE = delegatedTypeMapper(STRING, { it?.let { Locale.forLanguageTag(it) } }, { it?.toLanguageTag() })
	val COLOR = delegatedTypeMapper(INTEGER, { it?.let { Color(it, true) }  }, { it?.rgb })

	val ARRAY = object : TypeMapper<Any?, ByteArray> {
		fun Any.asArray(): Array<*> = when (this) {
			is Array<*> -> this
			is Collection<*> -> this.toTypedArray()
			else -> error("Invalid type")
		}

		fun Collection<*>.createArray(component: KType) = if (component.jvmErasure.java.isPrimitive) toTypedArray() else (this as java.util.Collection<*>).toArray { java.lang.reflect.Array.newInstance(component.jvmErasure.java, it) as Array<*> }

		override fun accepts(manager: DatabaseConnection, property: KProperty<*>?, type: KType): Boolean = type.isArray()
		override fun getType(column: ColumnData<*, *>?, table: TableStructure<*>, type: KType): DataType =
            SQLiteType.BLOB

		override fun <O: Any> initialize(column: ColumnData<O, *>, type: KType) {
			val component = type.component()
			val componentMapper = column.table.manager.getTypeMapper<Any, Any>(component, column.property)

			componentMapper.initialize(column, component)
		}

		override fun format(column: ColumnContext, table: TableStructure<*>, type: KType, value: Any?): ByteArray {
			if (value == null) return ByteArray(0)

			val component = type.component()
			val mapper = table.manager.getTypeMapper<Any?, Any?>(component, column.lastOrNull()?.property)

			val result = ByteArrayOutputStream()
			val array = value.asArray()

			ObjectOutputStream(result).use { stream ->
				stream.writeInt(array.size)
				array.forEach {
					val bytes = mapper.writeToBinary(column, table, component, it)
					stream.writeInt(bytes.size)
					stream.write(bytes)
				}
			}

			return result.toByteArray()
		}

		override fun extract(column: ColumnContext, type: KType, context: ReadContext, position: Int): ByteArray = context.read(position, ResultSet::getBytes)
		override fun parse(column: ColumnContext, type: KType, value: ByteArray, context: ReadContext, position: Int): Any? {
			if (value.isEmpty()) return null

			val component = type.component()
			val mapper = context.table.structure.manager.getTypeMapper<Any?, Any?>(component, column.lastOrNull()?.property)

			val array = ObjectInputStream(ByteArrayInputStream(value)).use { stream ->
				val size = stream.readInt()
				(1..size)
					.map { stream.readNBytes(stream.readInt()) }
					.toTypedArray()
			}

			return type.createCollection(array.map { mapper.readFromBinary(column, component, it, context, position) }.createArray(component))
		}

		override fun toBinary(column: ColumnContext, table: TableStructure<*>, type: KType, value: ByteArray): ByteArray = value
		override fun fromBinary(column: ColumnContext, type: KType, value: ByteArray, context: ReadContext, position: Int): ByteArray = value
	}
}

enum class SQLiteType(override val sqlName: String) : DataType {
	INTEGER("integer"),
	REAL("real"),
	TEXT("text"),
	BLOB("blob");

	override fun toString(): String = sqlName
}