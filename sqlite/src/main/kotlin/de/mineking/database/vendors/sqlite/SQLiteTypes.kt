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
	val BOOLEAN = nullsafeTypeMapper<Boolean>(SQLiteType.INTEGER, { set, position -> set.getInt(position) > 0 }, { statement, position, value -> statement.setInt(position, if (value) 1 else 0) })
	val BYTE_ARRAy = nullsafeTypeMapper<ByteArray?>(SQLiteType.BLOB, ResultSet::getBytes, PreparedStatement::setBytes)
	val BLOB = nullsafeTypeMapper<Blob?>(SQLiteType.BLOB, ResultSet::getBlob, PreparedStatement::setBlob)

	val SHORT = nullsafeTypeMapper<Short>(SQLiteType.INTEGER, ResultSet::getShort, PreparedStatement::setShort)
	val INTEGER = nullsafeTypeMapper<Int>(SQLiteType.INTEGER, ResultSet::getInt, PreparedStatement::setInt)
	val LONG = nullsafeTypeMapper<Long>(SQLiteType.INTEGER, ResultSet::getLong, PreparedStatement::setLong)

	val FLOAT = nullsafeTypeMapper<Float>(SQLiteType.REAL, ResultSet::getFloat, PreparedStatement::setFloat)
	val DOUBLE = nullsafeTypeMapper<Double>(SQLiteType.REAL, ResultSet::getDouble, PreparedStatement::setDouble)

	val STRING = nullsafeTypeMapper<String>(SQLiteType.TEXT, ResultSet::getString, PreparedStatement::setString)
	val ENUM = nullsafeDelegateTypeMapper<Enum<*>, String>(STRING, { name, type -> type.jvmErasure.java.enumConstants.map { it as Enum<*> }.first { it.name == name } }, Enum<*>::name)

	val INSTANT = nullsafeTypeMapper<Instant>(SQLiteType.INTEGER, { set, position -> set.getTimestamp(position).toInstant() }, { statement, position, value -> statement.setTimestamp(position, Timestamp.from(value)) })
	val LOCAL_DATE_TIME = nullsafeTypeMapper<LocalDateTime>(SQLiteType.INTEGER, { set, position -> set.getTimestamp(position).toLocalDateTime() }, { statement, position, value -> statement.setTimestamp(position, Timestamp.valueOf(value)) })
	val LOCAL_DATE = nullsafeTypeMapper<LocalDate>(SQLiteType.INTEGER, { set, position -> set.getDate(position).toLocalDate() }, { statement, position, value -> statement.setDate(position, Date.valueOf(value)) })

	val LOCALE = nullsafeDelegateTypeMapper(STRING, { it, type -> Locale.forLanguageTag(it) }, Locale::toLanguageTag)
	val COLOR = nullsafeDelegateTypeMapper(INTEGER, { it, type -> Color(it, true) }, Color::getRGB)

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
		override fun getType(column: ColumnData<*, *>?, table: TableStructure<*>, type: KType): DataType = SQLiteType.TEXT.withNullability(type.isMarkedNullable)

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

	val ARRAY = object : TypeMapper<Any?, ByteArray> {
		fun Any.asArray(): Array<*> = when (this) {
			is Array<*> -> this
			is Collection<*> -> this.toTypedArray()
			else -> error("Invalid type")
		}

		fun Collection<*>.createArray(component: KType) = if (component.jvmErasure.java.isPrimitive) toTypedArray() else (this as java.util.Collection<*>).toArray { java.lang.reflect.Array.newInstance(component.jvmErasure.java, it) as Array<*> }

		override fun accepts(manager: DatabaseConnection, property: KProperty<*>?, type: KType): Boolean = type.isArray()
		override fun getType(column: ColumnData<*, *>?, table: TableStructure<*>, type: KType): DataType = SQLiteType.BLOB.withNullability(type.isMarkedNullable)

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