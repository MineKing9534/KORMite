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
	val ANY = typeMapper<Any>(DataType.VALUE, { _, _ -> error("No suitable TypeMapper for insertion found") }, { value, statement, pos -> statement.setObject(pos, value) })

	val BOOLEAN = nullSafeTypeMapper<Boolean>(SQLiteType.INTEGER, { set, name -> set.getInt(name) > 0 }, { value, statement, position -> statement.setInt(position, if (value) 1 else 0) })
	val BYTE_ARRAy = typeMapper<ByteArray?>(SQLiteType.BLOB, { set, name -> set.getBytes(name) })
	val BLOB = typeMapper<Blob?>(SQLiteType.BLOB, { set, name -> set.getBlob(name) })

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

		override fun format(column: ColumnData<*, *>?, table: TableStructure<*>, type: KType, value: Enum<*>?): String? = value?.name

		override fun extract(column: DirectColumnData<*, *>?, type: KType, context: ReadContext, name: String): String? = STRING.extract(column, type, context, name)
		override fun parse(column: DirectColumnData<*, *>?, type: KType, value: String?, context: ReadContext, name: String): Enum<*>? = value?.let { name -> type.jvmErasure.java.enumConstants.map { it as Enum<*> }.first { it.name == name } }
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

		override fun format(column: ColumnData<*, *>?, table: TableStructure<*>, type: KType, value: Any?): String? = value?.let { gson.toJson(value) }
		override fun createArgument(column: ColumnData<*, *>?, table: TableStructure<*>, type: KType, value: String?): Argument = object : Argument {
			override fun apply(position: Int, statement: PreparedStatement?, ctx: StatementContext?) {
				if (value == null) statement?.setNull(position, Types.NULL)
				else statement?.setString(position, value)
			}

			override fun toString(): String = value.toString()
		}

		override fun extract(column: DirectColumnData<*, *>?, type: KType, context: ReadContext, name: String): String? = STRING.extract(column, type, context, name)
		override fun parse(column: DirectColumnData<*, *>?, type: KType, value: String?, context: ReadContext, name: String): Any? = value?.let { gson.fromJson(it, type.javaType) }
	}

	val INSTANT = typeMapper<Instant?>(SQLiteType.INTEGER, { set, name -> set.getTimestamp(name).toInstant() }, { value, statement, position -> statement.setTimestamp(position, value?.let { Timestamp.from(it) }) })
	val LOCAL_DATE_TIME = typeMapper<LocalDateTime?>(SQLiteType.INTEGER, { set, name -> set.getTimestamp(name).toLocalDateTime() }, { value, statement, position -> statement.setTimestamp(position, value?.let { Timestamp.valueOf(it) }) })
	val LOCAL_DATE = typeMapper<LocalDate?>(SQLiteType.INTEGER, { set, name -> set.getDate(name).toLocalDate() }, { value, statement, position -> statement.setDate(position, value?.let { Date.valueOf(it) }) })

	val LOCALE = typeMapper(STRING, { it?.let { Locale.forLanguageTag(it) } }, { it?.toLanguageTag() })
	val COLOR = typeMapper(INTEGER, { it?.let { Color(it, true) }  }, { it?.rgb })

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

		override fun <O: Any> initialize(column: DirectColumnData<O, *>, type: KType) {
			val component = type.component()
			val componentMapper = column.table.manager.getTypeMapper<Any, Any>(component, column.property) ?: throw IllegalArgumentException("No TypeMapper found for $component")

			componentMapper.initialize(column, component)
		}

		override fun format(column: ColumnData<*, *>?, table: TableStructure<*>, type: KType, value: Any?): ByteArray {
			if (value == null) return ByteArray(0)

			val component = type.component()
			val mapper = table.manager.getTypeMapper<Any?, Any?>(component, if (column is DirectColumnData) column.property else null) ?: throw IllegalArgumentException("No TypeMapper found for $component")

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

		override fun extract(column: DirectColumnData<*, *>?, type: KType, context: ReadContext, name: String): ByteArray = context.read(name, ResultSet::getBytes)
		override fun parse(column: DirectColumnData<*, *>?, type: KType, value: ByteArray, context: ReadContext, name: String): Any? {
			if (value.isEmpty()) return null

			val component = type.component()
			val mapper = context.table.manager.getTypeMapper<Any?, Any?>(component, if (column is DirectColumnData) column.property else null) ?: throw IllegalArgumentException("No TypeMapper found for $component")

			val array = ObjectInputStream(ByteArrayInputStream(value)).use { stream ->
				val size = stream.readInt()
				(1..size)
					.map { stream.readNBytes(stream.readInt()) }
					.toTypedArray()
			}

			if (column?.reference == null) return type.createCollection(array.map { mapper.readFromBinary(column, component, it, context, name) }.createArray(component))
			else {
				@Suppress("UNCHECKED_CAST")
				val key = column.reference!!.structure.getKeys()[0] as ColumnData<Any, Any>

				val ids = array.map { key.mapper.readFromBinary(column, key.type, it, context, name) }
				val rows = column.reference!!.select(where = property<Any>(key.name).isIn(ids.map { value(it, key.type) })).list().associateBy { key.get(it) }
				println(rows)

				return type.createCollection(ids.map { rows[it] }.asArray())
			}
		}

		override fun toBinary(column: ColumnData<*, *>?, table: TableStructure<*>, type: KType, value: ByteArray): ByteArray = value
		override fun fromBinary(column: DirectColumnData<*, *>?, type: KType, value: ByteArray, context: ReadContext, name: String): ByteArray = value
	}

	val REFERENCE = object : TypeMapper<Any?, Any?> {
		override fun accepts(manager: DatabaseConnection, property: KProperty<*>?, type: KType): Boolean = property?.hasDatabaseAnnotation<Reference>() == true && !type.isArray()

		override fun <O: Any> initialize(column: DirectColumnData<O, *>, type: KType) {
			val table = column.property.getDatabaseAnnotation<Reference>()?.table ?: throw IllegalArgumentException("No table specified")

			val reference = column.table.manager.getCachedTable<Any>(table)
			column.reference = reference

			require(reference.structure.getKeys().size == 1) { "Can only reference a table with exactly one key" }
		}

		override fun getType(column: ColumnData<*, *>?, table: TableStructure<*>, type: KType): DataType {
			require(column is DirectColumnData) { "Something went really wrong" }

			val key = column.reference!!.structure.getKeys().first()
			return key.mapper.getType(column, table, key.type)
		}

		@Suppress("UNCHECKED_CAST")
		override fun format(column: ColumnData<*, *>?, table: TableStructure<*>, type: KType, value: Any?): Any? {
			require(column is DirectColumnData) { "Cannot use references for virtual columns" }

			val reference = column.reference!! as Table<Any>
			val key = reference.structure.getKeys().first() as DirectColumnData<Any, Any>

			return value?.let { key.mapper.format(column, reference.structure, key.type, key.get(it )) }
		}

		@Suppress("UNCHECKED_CAST")
		override fun createArgument(column: ColumnData<*, *>?, table: TableStructure<*>, type: KType, value: Any?): Argument {
			require(column is DirectColumnData) { "Cannot use references for virtual columns" }

			val reference = column.reference!! as Table<Any>
			val key = reference.structure.getKeys().first() as DirectColumnData<Any, Any?>

			return key.mapper.write(key, reference.structure, key.type, value)
		}

		override fun extract(column: DirectColumnData<*, *>?, type: KType, context: ReadContext, name: String): Any? = null

		@Suppress("UNCHECKED_CAST")
		override fun parse(column: DirectColumnData<*, *>?, type: KType, value: Any?, context: ReadContext, name: String): Any? {
			require(column != null) { "Cannot parse reference without column context" }

			val reference = column.reference!! as Table<Any>
			val key = reference.structure.getKeys().first() as DirectColumnData<Any, Any?>

			val context = context.nest(column.name, column.reference!!.implementation)
			if (key.mapper.read(column, key.type, context, key.name) == null) return null

			column.reference!!.implementation.parseResult(context)

			return context.instance
		}
	}
}

enum class SQLiteType(override val sqlName: String) : DataType {
	INTEGER("integer"),
	REAL("real"),
	TEXT("text"),
	BLOB("blob");

	override fun toString(): String = sqlName
}