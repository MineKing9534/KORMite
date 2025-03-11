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
	val nullable: Boolean get() = false

	companion object {
		fun of(name: String): DataType = object : DataType {
			override val sqlName: String = name
			override fun toString(): String = name
		}
	}

	fun withNullability(nullable: Boolean) = object : DataType by this {
		override val nullable get() = nullable
	}
}

interface TypeMapper<T, D> {
	fun accepts(manager: DatabaseConnection, property: KProperty<*>?, type: KType): Boolean
	fun getType(column: PropertyData<*, *>?, table: TableStructure<*>, type: KType): DataType

	fun <O: Any> initialize(column: PropertyData<O, *>, type: KType) {}
	fun select(query: QueryBuilder<*>, column: ColumnContext) { query.rawNode(property<Any?>(column.map { it.property })) }

	fun format(column: ColumnContext, table: TableStructure<*>, type: KType, value: T): D
	fun createArgument(column: ColumnContext, table: TableStructure<*>, type: KType, value: D): Argument = object : Argument {
		override fun apply(position: Int, statement: PreparedStatement?, ctx: StatementContext?) {
			if (value == null) statement?.setNull(position, Types.NULL)
			else statement?.setObject(position, value)
		}

		override fun toString(): String = value.toString()
	}

	fun extract(column: ColumnContext, type: KType, context: ReadContext, pos: Int): D
	fun parse(column: ColumnContext, type: KType, value: D, context: ReadContext, pos: Int): T

	fun write(column: ColumnContext, table: TableStructure<*>, type: KType, value: T): Argument = createArgument(column, table, type, format(column, table, type, value))
	fun read(column: ColumnContext, type: KType, context: ReadContext, pos: Int): T = parse(column, type, extract(column, type, context, pos), context, pos)

	fun writeToBinary(column: ColumnContext, table: TableStructure<*>, type: KType, value: T): ByteArray = toBinary(column, table, type, format(column, table, type, value))
	fun toBinary(column: ColumnContext, table: TableStructure<*>, type: KType, value: D): ByteArray {
		val output = ByteArrayOutputStream()

		ObjectOutputStream(output).use { it.writeObject(value) }

		return output.toByteArray()
	}

	@Suppress("UNCHECKED_CAST")
	fun fromBinary(column: ColumnContext, type: KType, value: ByteArray, context: ReadContext, pos: Int): D = ObjectInputStream(ByteArrayInputStream(value)).use { it.readObject() } as D
	fun readFromBinary(column: ColumnContext, type: KType, value: ByteArray, context: ReadContext, pos: Int): T = parse(column, type, fromBinary(column, type, value, context, pos), context, pos)
}

interface SimpleTypeMapper<T> : TypeMapper<T, T> {
	override fun format(column: ColumnContext, table: TableStructure<*>, type: KType, value: T): T = value
	override fun parse(column: ColumnContext, type: KType, value: T, context: ReadContext, pos: Int): T = value
}

inline fun <reified T> typeMapper(
	dataType: DataType,
	noinline extractor: (ResultSet, Int) -> T?,
	crossinline inserter: (PreparedStatement, Int, T?) -> Unit,
	crossinline acceptor: (KType) -> Boolean = { it.isSubtypeOf(typeOf<T?>()) }
) = object : SimpleTypeMapper<T?> {
	override fun accepts(manager: DatabaseConnection, property: KProperty<*>?, type: KType) = acceptor(type)
	override fun getType(column: PropertyData<*, *>?, table: TableStructure<*>, type: KType) = dataType.withNullability(type.isMarkedNullable)

	override fun createArgument(column: ColumnContext, table: TableStructure<*>, type: KType, value: T?) = object : Argument {
		override fun apply(position: Int, statement: PreparedStatement?, ctx: StatementContext?) {
			inserter(statement!!, position, value)
		}

		override fun toString(): String = value.toString()
	}

	override fun extract(column: ColumnContext, type: KType, context: ReadContext, pos: Int) = context.read(pos, extractor)

	override fun toString() = "TypeMapper[${ typeOf<T>() }]"
}

inline fun <reified T> nullsafeTypeMapper(
    dataType: DataType,
    noinline extractor: (ResultSet, Int) -> T?,
    crossinline inserter: (PreparedStatement, Int, T) -> Unit,
    crossinline acceptor: (KType) -> Boolean = { it.isSubtypeOf(typeOf<T?>()) }
) = typeMapper<T?>(dataType, { set, pos ->
	if (set.getObject(pos) == null) null
	else extractor(set, pos)
}, { stmt, pos, value ->
	if (value == null) stmt.setNull(pos, Types.NULL)
	else inserter(stmt, pos, value)
}, acceptor)

inline fun <reified T, reified D> delegateTypeMapper(
    delegate: TypeMapper<D, *>,
    crossinline fromOther: (D, KType) -> T,
    crossinline toOther: (T) -> D
) = object : TypeMapper<T, D> {
	override fun accepts(manager: DatabaseConnection, property: KProperty<*>?, type: KType) = type.isSubtypeOf(typeOf<T>())
	override fun getType(column: PropertyData<*, *>?, table: TableStructure<*>, type: KType): DataType = delegate.getType(column, table, type)

	override fun <O : Any> initialize(column: PropertyData<O, *>, type: KType) = delegate.initialize(column, type)
	override fun select(query: QueryBuilder<*>, column: ColumnContext) = delegate.select(query, column)

	override fun format(column: ColumnContext, table: TableStructure<*>, type: KType, value: T): D = toOther(value)
	override fun createArgument(column: ColumnContext, table: TableStructure<*>, type: KType, value: D): Argument = delegate.write(column, table, type, value)

	override fun extract(column: ColumnContext, type: KType, context: ReadContext, pos: Int): D = delegate.read(column, type, context, pos)
	override fun parse(column: ColumnContext, type: KType, value: D, context: ReadContext, pos: Int): T = fromOther(value, type)

	override fun toString() = "DelegatedTypeMapper[$delegate -> ${ typeOf<T>() }]"
}

inline fun <reified T, reified D> nullsafeDelegateTypeMapper(
	delegate: TypeMapper<D?, *>,
	crossinline fromOther: (D, KType) -> T?,
	crossinline toOther: (T) -> D?
) = delegateTypeMapper(delegate, { it, type -> it?.let { fromOther(it, type) } }, { it?.let { toOther(it) } })

inline fun <reified T> binaryTypeMapper(
	dataType: DataType,
	crossinline parser: (ByteArray, KType) -> T?,
	crossinline formatter: (T?) -> ByteArray
) = object : TypeMapper<T?, ByteArray> {
	override fun accepts(manager: DatabaseConnection, property: KProperty<*>?, type: KType): Boolean = type.isSubtypeOf(typeOf<T>())
	override fun getType(column: PropertyData<*, *>?, table: TableStructure<*>, type: KType): DataType = dataType

	override fun format(column: ColumnContext, table: TableStructure<*>, type: KType, value: T?): ByteArray = formatter(value)

	override fun extract(column: ColumnContext, type: KType, context: ReadContext, position: Int): ByteArray = context.read(position, ResultSet::getBytes)
	override fun parse(column: ColumnContext, type: KType, value: ByteArray, context: ReadContext, position: Int): T? = parser(value, type)

	override fun toBinary(column: ColumnContext, table: TableStructure<*>, type: KType, value: ByteArray): ByteArray = value
	override fun fromBinary(column: ColumnContext, type: KType, value: ByteArray, context: ReadContext, position: Int): ByteArray = value
}

object ValueTypeMapper : SimpleTypeMapper<Any?> {
	override fun accepts(manager: DatabaseConnection, property: KProperty<*>?, type: KType): Boolean = true
	override fun getType(column: PropertyData<*, *>?, table: TableStructure<*>, type: KType): DataType = error("No suitable TypeMapper found for $type [${ column?.property }] (Cannot use value mapper in this context)")

	override fun extract(column: ColumnContext, type: KType, context: ReadContext, pos: Int): Any? = context.read(pos, ResultSet::getObject)
}