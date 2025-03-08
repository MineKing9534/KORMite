package de.mineking.database

import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.Jdbi
import org.jdbi.v3.core.kotlin.inTransactionUnchecked
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import java.lang.reflect.Proxy
import kotlin.reflect.*
import kotlin.reflect.full.memberProperties
import kotlin.reflect.jvm.javaField

internal val CURRENT_TRANSACTION: ThreadLocal<Handle> = ThreadLocal()

inline fun <reified T: Annotation> KProperty<*>.getDatabaseAnnotation(): T? = this.javaField?.getAnnotation(T::class.java)
inline fun <reified T: Annotation> KProperty<*>.hasDatabaseAnnotation(): Boolean = getDatabaseAnnotation<T>() != null

inline fun <reified T> createDummy() = Proxy.newProxyInstance(DatabaseConnection::class.java.classLoader, arrayOf(T::class.java)) { _, _, _ -> } as T

abstract class DatabaseConnection(
    @Suppress("UNUSED") val driver: Jdbi,
    val defaultNamingStrategy: NamingStrategy
) {
    val data: MutableMap<String, Any> = mutableMapOf()
    val typeMappers: MutableList<TypeMapper<*, *>> = arrayListOf(ValueTypeMapper)
    val annotationHandlers: MutableList<AnnotationHandler> = DefaultAnnotationHandlers::class.memberProperties.map { it.get(DefaultAnnotationHandlers) as AnnotationHandler }.toMutableList()

    var autoGenerate: (ColumnData<*, *>) -> String = { error("No default autogenerate configured") }

    private val tables = hashMapOf<String, Table<*>>()

    @Suppress("UNCHECKED_CAST")
    fun <T> data(name: String) = data[name] as T

    @Suppress("UNCHECKED_CAST")
    private fun <T: Any, W: Table<T>> createTableInstance(tableType: KClass<W>, structure: TableStructure<T>, instance: () -> T) = Proxy.newProxyInstance(tableType.java.classLoader, arrayOf(tableType.java), createTableImplementation(tableType, structure, instance)) as W
    protected abstract fun <T: Any> createTableImplementation(tableType: KClass<*>, structure: TableStructure<T>, instance: () -> T): TableImplementation<T>

    @Suppress("UNCHECKED_CAST")
    //findLast because mappers that were added later should have priority
    fun <T, D> getTypeMapper(type: KType, property: KProperty<*>?) = typeMappers.findLast { it.accepts(this, property, type) } as TypeMapper<T, D>? ?: throw IllegalArgumentException("No suitable TypeMapper found for $type [$property]")

    inline fun <reified T> getTypeMapper() = getTypeMapper<T, Any?>(typeOf<T>(), null)

    fun <T: Any> getTableStructure(
        type: KClass<T>,
        name: String = type.simpleName ?: throw IllegalArgumentException("You have to provide a table name when using anonymous classes!"),
        namingStrategy: NamingStrategy = defaultNamingStrategy
    ): TableStructure<T> {
        val columns = arrayListOf<ColumnData<T, *>>()
        val table = TableStructure(this, name, namingStrategy, columns, type)

        fun <C> createColumn(property: KProperty1<T, C>): ColumnData<T, C> {
            val nameOverride = property.getDatabaseAnnotation<Column>()?.name?.takeIf { it.isNotBlank() }
            return ColumnData(
                table,
                nameOverride ?: property.name,
                nameOverride ?: namingStrategy.getName(property.name),
                getTypeMapper<C, Any>(property.returnType, property),
                property,
                property.hasDatabaseAnnotation<Key>(),
                property.hasDatabaseAnnotation<AutoGenerate>() || property.hasDatabaseAnnotation<AutoIncrement>()
            )
        }

        columns += type.memberProperties
            .filter { it.javaField != null && it.hasDatabaseAnnotation<Column>() }
            .map { createColumn(it) }
            .sortedBy { !it.key }

        columns.forEach {
            fun <A: Any, B> init(column: ColumnData<A, B>) = column.mapper.initialize(column, column.type)
            init(it)
        }

        require(columns.isNotEmpty()) { "Cannot create table with no columns" }

        return table
    }

    inline fun <reified T: Any, reified C: Table<T>> getTable(
	    namingStrategy: NamingStrategy = defaultNamingStrategy,
	    name: String = namingStrategy.getName(T::class.simpleName ?: throw IllegalArgumentException("You have to provide a table name when using anonymous classes!")),
	    create: Boolean = true,
	    noinline instance: () -> T
    ) = getTable(T::class, C::class, namingStrategy, name, create, instance)

    fun <T: Any, C: Table<T>> getTable(
        type: KClass<T>,
        @Suppress("UNCHECKED_CAST")
        table: KClass<C>,
        namingStrategy: NamingStrategy = defaultNamingStrategy,
        name: String = namingStrategy.getName(type.simpleName ?: throw IllegalArgumentException("You have to provide a table name when using anonymous classes!")),
        create: Boolean = true,
        instance: () -> T
    ): C {
        require(name !in tables) { "Table for $type already registered - Use getCachedTable" }

        val structure = getTableStructure(type, name, namingStrategy)
        val tableInstance = createTableInstance(table, structure, instance)

        tables[name] = tableInstance
        typeMappers += tableInstance.implementation.mapper

        if (create) tableInstance.implementation.createTable()

        return tableInstance
    }

    @Suppress("UNCHECKED_CAST")
    fun <T: Any> getDefaultTable(
        type: KClass<T>,
        namingStrategy: NamingStrategy = defaultNamingStrategy,
        name: String = namingStrategy.getName(type.simpleName ?: throw IllegalArgumentException("You have to provide a table name when using anonymous classes!")),
        create: Boolean = true,
        instance: () -> T
    ) = getTable(type, DefaultTable::class as KClass<DefaultTable<T>>, namingStrategy, name, create, instance)

    inline fun <reified T: Any> getDefaultTable(
        namingStrategy: NamingStrategy = defaultNamingStrategy,
        name: String = namingStrategy.getName(T::class.simpleName ?: throw IllegalArgumentException("You have to provide a table name when using anonymous classes!")),
        create: Boolean = true,
        noinline instance: () -> T
    ) = getTable<T, DefaultTable<T>>(namingStrategy, name, create, instance)

    @Suppress("UNCHECKED_CAST")
    fun <T: Any> getCachedTable(name: String): Table<T> = tables[name] as Table<T>? ?: throw IllegalArgumentException("Table $name not found")

    fun <R> inTransaction(action: (Handle) -> R): R = driver.inTransactionUnchecked { handle ->
        try {
            CURRENT_TRANSACTION.set(handle)
            action(handle)
        } finally {
            CURRENT_TRANSACTION.set(null)
        }
    }

    fun <R> execute(action: (Handle) -> R): R {
        val transaction = CURRENT_TRANSACTION.get()
        return if (transaction != null) action(transaction)
        else driver.withHandleUnchecked { action(it) }
    }
}

//TODO !!! nullability via typemapper for more flexibility