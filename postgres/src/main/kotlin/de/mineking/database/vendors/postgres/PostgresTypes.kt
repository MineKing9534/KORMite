package de.mineking.database.vendors.postgres

import com.google.gson.GsonBuilder
import com.google.gson.ToNumberStrategy
import de.mineking.database.*
import org.jdbi.v3.core.argument.Argument
import org.jdbi.v3.core.statement.StatementContext
import org.postgresql.util.PGobject
import java.awt.Color
import java.math.BigDecimal
import java.math.BigInteger
import java.sql.*
import java.sql.Date
import java.time.*
import java.util.*
import kotlin.math.max
import kotlin.reflect.KProperty
import kotlin.reflect.KType
import kotlin.reflect.jvm.javaType
import kotlin.reflect.jvm.jvmErasure

object PostgresMappers {
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
		override fun getType(column: ColumnData<*, *>?, table: TableStructure<*>, type: KType): DataType = PostgresType.TEXT

		override fun format(column: ColumnContext, table: TableStructure<*>, type: KType, value: Enum<*>?): String? = value?.name

		override fun extract(column: ColumnContext, type: KType, context: ReadContext, position: Int): String? = STRING.extract(column, type, context, position)
		override fun parse(column: ColumnContext, type: KType, value: String?, context: ReadContext, position: Int): Enum<*>? = value?.let { name -> type.jvmErasure.java.enumConstants.map { it as Enum<*> }.first { it.name == name } }
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

		override fun accepts(manager: DatabaseConnection, property: KProperty<*>?, type: KType): Boolean = property?.hasDatabaseAnnotation<Json>() == true
		override fun getType(column: ColumnData<*, *>?, table: TableStructure<*>, type: KType): DataType = if (column?.property?.getDatabaseAnnotation<Json>()?.binary == true) PostgresType.JSONB else PostgresType.JSON

		override fun format(column: ColumnContext, table: TableStructure<*>, type: KType, value: Any?): String? = value?.let { gson.toJson(value) }
		override fun createArgument(column: ColumnContext, table: TableStructure<*>, type: KType, value: String?): Argument = object : Argument {
			override fun apply(position: Int, statement: PreparedStatement?, ctx: StatementContext?) {
				if (value == null) statement?.setNull(position, Types.NULL)
				else {
					val obj = PGobject()
					obj.type = getType(column.lastOrNull(), table, type).sqlName
					obj.value = value
					statement?.setObject(position, obj)
				}
			}

			override fun toString(): String = value.toString()
		}

		override fun extract(column: ColumnContext, type: KType, context: ReadContext, position: Int): String? = STRING.extract(column, type, context, position)
		override fun parse(column: ColumnContext, type: KType, value: String?, context: ReadContext, position: Int): Any? = value?.let { gson.fromJson(it, type.javaType) }
	}

	val INSTANT = typeMapper<Instant?>(PostgresType.TIMESTAMP, { set, name -> set.getTimestamp(name).toInstant() }, { value, statement, position -> statement.setTimestamp(position, value?.let { Timestamp.from(it) }) })
	val LOCAL_DATE_TIME = typeMapper<LocalDateTime?>(PostgresType.TIMESTAMP, { set, name -> set.getTimestamp(name).toLocalDateTime() }, { value, statement, position -> statement.setTimestamp(position, value?.let { Timestamp.valueOf(it) }) })
	val OFFSET_DATE_TIME = typeMapper<OffsetDateTime?>(PostgresType.TIMESTAMPTZ, { set, name -> set.getObject(name, OffsetDateTime::class.java) }, { value, statement, position -> statement.setTimestamp(position, value?.let { Timestamp.valueOf(it.toLocalDateTime()) }) })
	val ZONED_DATE_TIME = typeMapper<ZonedDateTime?>(PostgresType.TIMESTAMPTZ, { set, name -> set.getObject(name, OffsetDateTime::class.java).toZonedDateTime() }, { value, statement, position -> statement.setTimestamp(position, value?.let { Timestamp.valueOf(it.toLocalDateTime()) }) })
	val LOCAL_DATE = typeMapper<LocalDate?>(PostgresType.DATE, { set, name -> set.getDate(name).toLocalDate() }, { value, statement, position -> statement.setDate(position, value?.let { Date.valueOf(it) }) })

	val LOCALE = delegatedTypeMapper(STRING, { it?.let { Locale.forLanguageTag(it) } }, { it?.toLanguageTag() })
	val COLOR = delegatedTypeMapper(INTEGER, { it?.let { Color(it, true) }  }, { it?.rgb })

	val ARRAY = object : TypeMapper<Any?, Array<*>?> {
		fun Any.asArray(): Array<*> = when (this) {
			is Array<*> -> this
			is Collection<*> -> this.toTypedArray()
			else -> error("Invalid type")
		}

		fun Collection<*>.createArray(component: KType) = if (component.jvmErasure.java.isPrimitive) toTypedArray() else (this as java.util.Collection<*>).toArray { java.lang.reflect.Array.newInstance(component.jvmErasure.java, it) as Array<*> }

		override fun accepts(manager: DatabaseConnection, property: KProperty<*>?, type: KType): Boolean = type.isArray()
		override fun getType(column: ColumnData<*, *>?, table: TableStructure<*>, type: KType): DataType {
			val component = type.component()
			val componentMapper = table.manager.getTypeMapper<Any, Any>(component, column?.property)
			val componentType = componentMapper.getType(column, table, component)
			return DataType.of("${ componentType.sqlName }[]")
		}

		override fun <O: Any> initialize(column: ColumnData<O, *>, type: KType) {
			val component = type.component()
			val componentMapper = column.table.manager.getTypeMapper<Any, Any>(component, column.property)

			componentMapper.initialize(column, component)
		}

		override fun format(column: ColumnContext, table: TableStructure<*>, type: KType, value: Any?): Array<*>? {
			if (value == null) return null

			val component = type.component()
			val mapper = table.manager.getTypeMapper<Any?, Any?>(component, column.lastOrNull()?.property)

			return if (component.isArray()) {
				var maxLength = 0
				val array = value.asArray()
					.map { mapper.format(column, table, component, it) }
					.map { if (it == null) emptyArray<Any>() else it as Array<*> }
					.onEach { maxLength = max(maxLength, it.size) }

				array.map { it.copyOf(maxLength) as Array<*> }.toTypedArray()
			} else value.asArray()
				.map { mapper.format(column, table, component, it) }
				.toTypedArray()
		}

		override fun createArgument(column: ColumnContext, table: TableStructure<*>, type: KType, value: Array<*>?): Argument = object : Argument {
			override fun apply(position: Int, statement: PreparedStatement?, ctx: StatementContext?) {
				if (value == null) statement?.setNull(position, Types.NULL)
				else {
					val component = type.component()
					val mapper = table.manager.getTypeMapper<Any, Any>(component, column.lastOrNull()?.property)

					statement?.setArray(position, statement.connection.createArrayOf(mapper.getType(column.lastOrNull(), table, component).sqlName.replace("\\[]+$".toRegex(), ""), value))
				}
			}

			override fun toString(): String = value.contentDeepToString()
		}

		override fun extract(column: ColumnContext, type: KType, context: ReadContext, position: Int): Array<*>? = context.read(position, ResultSet::getArray)?.let { it.array as Array<*> }
		override fun parse(column: ColumnContext, type: KType, value: Array<*>?, context: ReadContext, position: Int): Any? {
			if (value == null) return null

			val component = type.component()
			val mapper = context.table.structure.manager.getTypeMapper<Any?, Any?>(component, column.lastOrNull()?.property)

			return type.createCollection(value
				.filter { component.isArray() || it != null }
				.map { mapper.parse(column, component, it, context, position) }
				.createArray(component)
			)
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
