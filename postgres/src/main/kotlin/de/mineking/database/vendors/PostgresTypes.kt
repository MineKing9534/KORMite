package de.mineking.database.vendors

import com.google.gson.GsonBuilder
import com.google.gson.ToNumberStrategy
import de.mineking.database.*
import org.jdbi.v3.core.argument.Argument
import org.jdbi.v3.core.statement.StatementContext
import java.math.BigDecimal
import java.math.BigInteger
import java.sql.*
import java.sql.Date
import java.time.*
import java.util.*
import kotlin.math.max
import kotlin.reflect.KProperty
import kotlin.reflect.KType
import kotlin.reflect.KTypeProjection
import kotlin.reflect.KVariance
import kotlin.reflect.full.createType
import kotlin.reflect.jvm.javaType
import kotlin.reflect.jvm.jvmErasure

object PostgresMappers {
	val ANY = typeMapper<Any>(DataType.VALUE, { _, _ -> error("No suitable TypeMapper for insertion found") }, { value, statement, pos -> statement.setObject(pos, value) })

	val BOOLEAN = nullSafeTypeMapper<Boolean>(PostgresType.BOOLEAN, ResultSet::getBoolean)
	val BYTE_ARRAy = typeMapper<ByteArray?>(PostgresType.BYTE_ARRAY, { set, name -> set.getBytes(name) })
	val BLOB = typeMapper<Blob?>(PostgresType.BYTE_ARRAY, { set, name -> set.getBlob(name) })

	val SHORT = nullSafeTypeMapper<Short>(PostgresType.SMALL_INT, ResultSet::getShort)
	val INTEGER = nullSafeTypeMapper<Int>(PostgresType.INTEGER, ResultSet::getInt)
	val LONG = nullSafeTypeMapper<Long>(PostgresType.BIG_INT, ResultSet::getLong)
	val BIG_INTEGER = typeMapper<BigInteger>(PostgresType.BIG_INT, { set, name -> set.getObject(name, BigInteger::class.java) })

	val FLOAT = nullSafeTypeMapper<Float>(PostgresType.REAL, ResultSet::getFloat)
	val DOUBLE = nullSafeTypeMapper<Double>(PostgresType.DOUBLE_PRECISION, ResultSet::getDouble)
	val BIG_DECIMAL = typeMapper<BigDecimal>(PostgresType.NUMERIC, ResultSet::getBigDecimal)

	val STRING = typeMapper<String?>(PostgresType.TEXT, ResultSet::getString)
	val ENUM = object : TypeMapper<Enum<*>?, String?> {
		override fun accepts(manager: DatabaseConnection, property: KProperty<*>?, type: KType): Boolean = type.jvmErasure.java.isEnum
		override fun getType(column: ColumnData<*, *>?, table: TableStructure<*>, property: KProperty<*>?, type: KType): DataType = PostgresType.TEXT

		override fun format(column: ColumnData<*, *>?, table: TableStructure<*>, type: KType, value: Enum<*>?): String? = value?.name

		override fun extract(column: DirectColumnData<*, *>?, type: KType, context: ReadContext, name: String): String? = STRING.extract(column, type, context, name)
		override fun parse(column: DirectColumnData<*, *>?, type: KType, value: String?, context: ReadContext, name: String): Enum<*>? = value?.let { name -> type.jvmErasure.java.enumConstants.map { it as Enum<*> }.first { it.name == name } }
	}

	val UUID_MAPPER = typeMapper<UUID?>(PostgresType.UUID, { set, name -> UUID.fromString(set.getString(name)) }, { value, statement, pos -> statement.setObject(pos, value) })
	val JSON = object : TypeMapper<Any?, String?> {
		val numberStrategy = ToNumberStrategy { reader ->
			val str = reader!!.nextString()
			if (str.contains(".")) str.toDouble() else str.toInt()
		}
		val gson = GsonBuilder()
			.setNumberToNumberStrategy(numberStrategy)
			.setObjectToNumberStrategy(numberStrategy)
			.create()

		override fun accepts(manager: DatabaseConnection, property: KProperty<*>?, type: KType): Boolean = property?.hasDatabaseAnnotation<JSON>() == true
		override fun getType(column: ColumnData<*, *>?, table: TableStructure<*>, property: KProperty<*>?, type: KType): DataType = if (property?.getDatabaseAnnotation<JSON>()?.decompress == true) PostgresType.JSONB else PostgresType.JSON

		override fun format(column: ColumnData<*, *>?, table: TableStructure<*>, type: KType, value: Any?): String? = value?.let { gson.toJson(value) }

		override fun extract(column: DirectColumnData<*, *>?, type: KType, context: ReadContext, name: String): String? = STRING.extract(column, type, context, name)
		override fun parse(column: DirectColumnData<*, *>?, type: KType, value: String?, context: ReadContext, name: String): Any? = value?.let { gson.fromJson(it, type.javaType) }
	}

	val INSTANT = typeMapper<Instant?>(PostgresType.TIMESTAMP, { set, name -> set.getTimestamp(name).toInstant() }, { value, statement, position -> statement.setTimestamp(position, value?.let { Timestamp.from(it) }) })
	val LOCAL_DATE_TIME = typeMapper<LocalDateTime?>(PostgresType.TIMESTAMP, { set, name -> set.getTimestamp(name).toLocalDateTime() }, { value, statement, position -> statement.setTimestamp(position, value?.let { Timestamp.valueOf(it) }) })
	val OFFSET_DATE_TIME = typeMapper<OffsetDateTime?>(PostgresType.TIMESTAMPTZ, { set, name -> set.getObject(name, OffsetDateTime::class.java) }, { value, statement, position -> statement.setTimestamp(position, value?.let { Timestamp.valueOf(it.toLocalDateTime()) }) })
	val ZONED_DATE_TIME = typeMapper<ZonedDateTime?>(PostgresType.TIMESTAMPTZ, { set, name -> set.getObject(name, OffsetDateTime::class.java).toZonedDateTime() }, { value, statement, position -> statement.setTimestamp(position, value?.let { Timestamp.valueOf(it.toLocalDateTime()) }) })
	val LOCAL_DATE = typeMapper<LocalDate?>(PostgresType.DATE, { set, name -> set.getDate(name).toLocalDate() }, { value, statement, position -> statement.setDate(position, value?.let { Date.valueOf(it) }) })

	val ARRAY = object : TypeMapper<Any?, Array<*>?> {
		fun Any.asArray(): Array<*> = when (this) {
			is Array<*> -> this
			is Collection<*> -> this.toTypedArray()
			else -> error("Invalid type")
		}

		fun Collection<*>.createArray(component: KType) = (this as java.util.Collection<*>).toArray { java.lang.reflect.Array.newInstance(component.jvmErasure.java, it) as Array<*> }

		override fun accepts(manager: DatabaseConnection, property: KProperty<*>?, type: KType): Boolean = type.isArray()
		override fun getType(column: ColumnData<*, *>?, table: TableStructure<*>, property: KProperty<*>?, type: KType): DataType {
			val component = type.component()
			val componentMapper = table.manager.getTypeMapper<Any, Any>(component, property) ?: throw IllegalArgumentException("No TypeMapper found for $component")
			val componentType = componentMapper.getType(column, table, property, component)
			return DataType.of("${ componentType.sqlName }[]")
		}

		override fun <O: Any> initialize(column: DirectColumnData<O, *>, type: KType) {
			val component = type.component()
			val componentMapper = column.table.manager.getTypeMapper<Any, Any>(component, column.property) ?: throw IllegalArgumentException("No TypeMapper found for $component")

			componentMapper.initialize(column, component)
		}

		override fun format(column: ColumnData<*, *>?, table: TableStructure<*>, type: KType, value: Any?): Array<*>? {
			if (value == null) return null

			val component = type.component()
			val mapper = table.manager.getTypeMapper<Any?, Any?>(component, if (column is DirectColumnData) column.property else null) ?: throw IllegalArgumentException("No TypeMapper found for $component")

			return if (component.isArray()) {
				var maxLength = 0
				val array = value.asArray()
					.map { mapper.format(column, table, component, it) }
					.map { if (it == null) emptyArray<Any>() else it as Array<*> }
					.onEach { maxLength = max(maxLength, it.size) }

				array.map { Arrays.copyOf(it, maxLength) as Array<*> }.toTypedArray()
			} else value.asArray()
				.map { mapper.format(column, table, component, it) }
				.toTypedArray()
		}

		override fun createArgument(column: ColumnData<*, *>?, table: TableStructure<*>, type: KType, value: Array<*>?): Argument = object : Argument {
			override fun apply(position: Int, statement: PreparedStatement?, ctx: StatementContext?) {
				if (value == null) statement?.setNull(position, Types.NULL)
				else {
					val component = type.component()
					val mapper = table.manager.getTypeMapper<Any, Any>(component, if (column is DirectColumnData) column.property else null) ?: throw IllegalArgumentException("No TypeMapper found for $component")

					statement?.setArray(position, statement.connection.createArrayOf(mapper.getType(column, table, null, component).sqlName.replace("\\[]+$".toRegex(), ""), value))
				}
			}

			override fun toString(): String = value.contentDeepToString()
		}

		override fun extract(column: DirectColumnData<*, *>?, type: KType, context: ReadContext, name: String): Array<*>? = context.read(name, ResultSet::getArray)?.let { it.array as Array<*> }
		override fun parse(column: DirectColumnData<*, *>?, type: KType, value: Array<*>?, context: ReadContext, name: String): Any? {
			if (value == null) return null

			if (column?.reference == null) {
				val component = type.component()
				val mapper = context.table.manager.getTypeMapper<Any?, Any?>(component, if (column is DirectColumnData) column.property else null) ?: throw IllegalArgumentException("No TypeMapper found for $component")

				return type.createCollection(value
					.filter { component.isArray() || it != null }
					.map { mapper.parse(column, component, it, context, name) }
					.createArray(component)
				)
			} else {
				@Suppress("UNCHECKED_CAST")
				val key = column.reference!!.structure.getKeys()[0] as ColumnData<Any, Any>

				val rows = column.reference!!.select(where = value(value.filterNotNull(), List::class.createType(arguments = listOf(KTypeProjection(KVariance.INVARIANT, key.type)))) contains property(key.name)).list().associateBy { key.get(it) }

				return type.createCollection(value.map { rows[it] }.asArray())
			}
		}
	}

	val REFERENCE = object : TypeMapper<Any?, Any?> {
		override fun accepts(manager: DatabaseConnection, property: KProperty<*>?, type: KType): Boolean = property?.hasDatabaseAnnotation<Reference>() == true && !type.isArray()

		override fun <O: Any> initialize(column: DirectColumnData<O, *>, type: KType) {
			val table = column.property.getDatabaseAnnotation<Reference>()?.table ?: throw IllegalArgumentException("No table specified")

			val reference = column.table.manager.getCachedTable<Any>(table)
			column.reference = reference

			require(reference.structure.getKeys().size == 1) { "Can only reference a table with exactly one key" }
		}

		override fun getType(column: ColumnData<*, *>?, table: TableStructure<*>, property: KProperty<*>?, type: KType): DataType {
			require(column is DirectColumnData) { "Something went really wrong" }

			val key = column.reference!!.structure.getKeys().first()
			return key.mapper.getType(column, table, property, key.type)
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

enum class PostgresType(override val sqlName: String) : DataType {
	INTEGER("integer"), //signed four-byte integer
	BIG_INT("bigint"), //signed eight-byte integer
	SMALL_INT("smallint"), //signed two-byte integer

	NUMERIC("numeric"), //exact numeric of selectable precision
	REAL("real"), //single precision floating-point number (4 bytes)
	DOUBLE_PRECISION("float8"), //double precision floating-point number (8 bytes)

	CHARACTER("character"), //fixed-length character string
	VAR_CHAR("varchar"), //variable-length character string
	TEXT("text"), //variable-length character string

	BOOLEAN("boolean"), //logical Boolean (true/false)
	BIT("bit"), //fixed-length bit string
	VAR_BIT("varbit"), //variable-length bit string
	BYTE_ARRAY("bytea"), //binary data ("byte array")

	TIME("time"), //time of day (no time zone)
	TIMETZ("timetz"), //time of day, including time zone
	TIMESTAMP("timestamp"), //date and time (no time zone)
	TIMESTAMPTZ("timestamptz"), //date and time, including time zone
	INTERVAL("interval"), //time span
	DATE("date"), //calendar date (year, month, day)

	MAC_ADDR("macaddr"), //MAC (Media Access Control) address
	MAC_ADDR_8("macaddr8"), //MAC (Media Access Control) address (EUI-64 format)
	CIDR("cidr"), //IPv4 or IPv6 network address
	INET("inet"), //IPv4 or IPv6 host address

	POINT("point"), //geometric point on a plane
	LINE("line"), //infinite line on a plane
	LINE_SEGMENT("lseg"), //line segment on a plane
	POLYGON("polygon"), //closed geometric path on a plane
	BOX("box"), //rectangular box on a plane
	CIRCLE("circle"), //circle on a plane
	PATH("path"), //geometric path on a plane

	XML("xml"),
	JSON("json"), //textual JSON data
	JSONB("jsonb"), //binary JSON data, decomposed

	UUID("uuid"), //universally unique identifier
	MONEY("money"), //currency amount
	PG_LSN("pg_lsn"), //PostgreSQL Log Sequence Number
	PG_SNAPSHOT("pg_snapshot"); //user-level transaction ID snapshot

	override fun toString(): String = sqlName
}
