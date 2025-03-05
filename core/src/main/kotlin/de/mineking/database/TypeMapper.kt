package de.mineking.database

import org.jdbi.v3.core.argument.Argument
import org.jdbi.v3.core.statement.StatementContext
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.Types
import kotlin.reflect.KProperty
import kotlin.reflect.KType
import kotlin.reflect.full.isSubtypeOf
import kotlin.reflect.typeOf

interface DataType {
	val sqlName: String

	companion object {
		fun of(name: String): DataType = object : DataType {
			override val sqlName: String = name
			override fun toString(): String = name
		}
	}
}

interface TypeMapper<T, D> {
	fun accepts(manager: DatabaseConnection, property: KProperty<*>?, type: KType): Boolean

	fun <O: Any> initialize(column: DirectColumnData<O, *>, type: KType) {}
	fun getType(column: ColumnData<*, *>?, table: TableStructure<*>, type: KType): DataType

	fun format(column: ColumnData<*, *>?, table: TableStructure<*>, type: KType, value: T): D
	fun createArgument(column: ColumnData<*, *>?, table: TableStructure<*>, type: KType, value: D): Argument = object : Argument {
		override fun apply(position: Int, statement: PreparedStatement?, ctx: StatementContext?) {
			if (value == null) statement?.setNull(position, Types.NULL)
			else statement?.setObject(position, value)
		}

		override fun toString(): String = value.toString()
	}

	fun extract(column: DirectColumnData<*, *>?, type: KType, context: ReadContext, name: String): D
	fun parse(column: DirectColumnData<*, *>?, type: KType, value: D, context: ReadContext, name: String): T

	fun write(column: ColumnData<*, *>?, table: TableStructure<*>, type: KType, value: T): Argument = createArgument(column, table, type, format(column, table, type, value))
	fun read(column: DirectColumnData<*, *>?, type: KType, context: ReadContext, name: String): T = parse(column, type, extract(column, type, context, name), context, name)

	fun writeToBinary(column: ColumnData<*, *>?, table: TableStructure<*>, type: KType, value: T): ByteArray = toBinary(column, table, type, format(column, table, type, value))
	fun toBinary(column: ColumnData<*, *>?, table: TableStructure<*>, type: KType, value: D): ByteArray {
		val output = ByteArrayOutputStream()

		ObjectOutputStream(output).use { it.writeObject(value) }

		return output.toByteArray()
	}

	@Suppress("UNCHECKED_CAST")
	fun fromBinary(column: DirectColumnData<*, *>?, type: KType, value: ByteArray, context: ReadContext, name: String): D = ObjectInputStream(ByteArrayInputStream(value)).use { it.readObject() } as D
	fun readFromBinary(column: DirectColumnData<*, *>?, type: KType, value: ByteArray, context: ReadContext, name: String): T = parse(column, type, fromBinary(column, type, value, context, name), context, name)
}

interface SimpleTypeMapper<T> : TypeMapper<T, T> {
	override fun format(column: ColumnData<*, *>?, table: TableStructure<*>, type: KType, value: T): T = value
	override fun parse(column: DirectColumnData<*, *>?, type: KType, value: T, context: ReadContext, name: String): T = value
}

inline fun <reified T> typeMapper(
	dataType: DataType,
	noinline extractor: (ResultSet, String) -> T,
	crossinline inserter: (T, PreparedStatement, Int) -> Unit = { value, statement, position ->
		if (value == null) statement.setNull(position, Types.NULL)
		else statement.setObject(position, value)
	},
	crossinline acceptor: (KType) -> Boolean = { it.isSubtypeOf(typeOf<T>()) }
): SimpleTypeMapper<T> = object : SimpleTypeMapper<T> {
	override fun accepts(manager: DatabaseConnection, property: KProperty<*>?, type: KType): Boolean = acceptor(type)
	override fun getType(column: ColumnData<*, *>?, table: TableStructure<*>, type: KType): DataType = dataType

	override fun createArgument(column: ColumnData<*, *>?, table: TableStructure<*>, type: KType, value: T): Argument = object : Argument {
		override fun apply(position: Int, statement: PreparedStatement?, ctx: StatementContext?) {
			inserter(value, statement!!, position)
		}

		override fun toString(): String = value.toString()
	}

	override fun extract(column: DirectColumnData<*, *>?, type: KType, context: ReadContext, name: String): T = context.read(name, extractor)

	override fun toString() = typeOf<T>().toString()
}

inline fun <reified T> nullSafeTypeMapper(
	dataType: DataType,
	noinline extractor: (ResultSet, String) -> T,
	crossinline inserter: (T, PreparedStatement, Int) -> Unit = { value, statement, position -> statement.setObject(position, value) },
	crossinline acceptor: (KType) -> Boolean = { it.isSubtypeOf(typeOf<T?>()) }
) = typeMapper<T?>(dataType, { set, name ->
	val temp = extractor(set, name)

	if (set.wasNull()) null
	else temp
}, { value, statement, position -> if (value == null) statement.setNull(position, Types.NULL) else inserter(value, statement, position) }, acceptor)

inline fun <reified T, reified D> typeMapper(
	temporary: TypeMapper<D, *>,
	crossinline parser: (D) -> T,
	crossinline formatter: (T) -> D
): TypeMapper<T, D> = object : TypeMapper<T, D> {
	override fun accepts(manager: DatabaseConnection, property: KProperty<*>?, type: KType): Boolean = type.isSubtypeOf(typeOf<T>())
	override fun getType(column: ColumnData<*, *>?, table: TableStructure<*>, type: KType): DataType = temporary.getType(column, table, type)

	override fun format(column: ColumnData<*, *>?, table: TableStructure<*>, type: KType, value: T): D = formatter(value)
	override fun createArgument(column: ColumnData<*, *>?, table: TableStructure<*>, type: KType, value: D): Argument = temporary.write(column, table, type, value)

	override fun extract(column: DirectColumnData<*, *>?, type: KType, context: ReadContext, name: String): D = temporary.read(column, type, context, name)
	override fun parse(column: DirectColumnData<*, *>?, type: KType, value: D, context: ReadContext, name: String): T = parser(value)

	override fun toString() = typeOf<T>().toString()
}

inline fun <reified T> binaryTypeMapper(
	dataType: DataType,
	crossinline parser: (ByteArray) -> T?,
	crossinline formatter: (T?) -> ByteArray
): TypeMapper<T?, ByteArray> = object : TypeMapper<T?, ByteArray> {
	override fun accepts(manager: DatabaseConnection, property: KProperty<*>?, type: KType): Boolean = type.isSubtypeOf(typeOf<T>())
	override fun getType(column: ColumnData<*, *>?, table: TableStructure<*>, type: KType): DataType = dataType

	override fun format(column: ColumnData<*, *>?, table: TableStructure<*>, type: KType, value: T?): ByteArray = formatter(value)

	override fun extract(column: DirectColumnData<*, *>?, type: KType, context: ReadContext, name: String): ByteArray = context.read(name, ResultSet::getBytes)
	override fun parse(column: DirectColumnData<*, *>?, type: KType, value: ByteArray, context: ReadContext, name: String): T? = parser(value)

	override fun toBinary(column: ColumnData<*, *>?, table: TableStructure<*>, type: KType, value: ByteArray): ByteArray = value
	override fun fromBinary(column: DirectColumnData<*, *>?, type: KType, value: ByteArray, context: ReadContext, name: String): ByteArray = value
}

object ValueTypeMapper : SimpleTypeMapper<Any?> {
	override fun accepts(
		manager: DatabaseConnection,
		property: KProperty<*>?,
		type: KType
	): Boolean = true

	override fun getType(
		column: ColumnData<*, *>?,
		table: TableStructure<*>,
		type: KType
	): DataType = error("No suitable TypeMapper found for $type")

	override fun extract(
		column: DirectColumnData<*, *>?,
		type: KType,
		context: ReadContext,
		name: String
	): Any? = context.read(name, ResultSet::getObject)
}