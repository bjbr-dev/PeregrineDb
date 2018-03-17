// <copyright file="TableSchemaFactory.cs" company="Berkeleybross">
// Copyright (c) Berkeleybross. All rights reserved.
// </copyright>
namespace Dapper.MicroCRUD.Schema
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Collections.Immutable;
    using System.ComponentModel.DataAnnotations;
    using System.ComponentModel.DataAnnotations.Schema;
    using System.Data;
    using System.Linq;
    using System.Reflection;
    using Dapper.MicroCRUD.Dialects;
    using Dapper.MicroCRUD.Utils;

    /// <summary>
    /// Methods to create an instance of a <see cref="TableSchema"/>.
    /// </summary>
    internal class TableSchemaFactory
    {
        private static readonly ConcurrentDictionary<TableSchemaCacheIdentity, TableSchema> Schemas =
            new ConcurrentDictionary<TableSchemaCacheIdentity, TableSchema>();

        private static readonly ConcurrentDictionary<ConditionsColumnCacheIdentity, ImmutableArray<ConditionColumnSchema>> ConditionColumns =
            new ConcurrentDictionary<ConditionsColumnCacheIdentity, ImmutableArray<ConditionColumnSchema>>();

        private readonly ITableNameFactory tableNameFactory;
        private readonly IColumnNameFactory columnNameFactory;

        /// <summary>
        /// Initializes a new instance of the <see cref="TableSchemaFactory"/> class.
        /// </summary>
        public TableSchemaFactory(
            ITableNameFactory tableNameFactory,
            IColumnNameFactory columnNameFactory)
        {
            Ensure.NotNull(tableNameFactory, nameof(tableNameFactory));
            Ensure.NotNull(columnNameFactory, nameof(columnNameFactory));

            this.tableNameFactory = tableNameFactory;
            this.columnNameFactory = columnNameFactory;
        }

        /// <summary>
        /// Gets or sets the types to convert to SQL types.
        /// </summary>
        internal static Dictionary<Type, DbType> TypeMapping { get; set; } = new Dictionary<Type, DbType>
            {
                [typeof(byte)] = DbType.Byte,
                [typeof(sbyte)] = DbType.SByte,
                [typeof(short)] = DbType.Int16,
                [typeof(ushort)] = DbType.UInt16,
                [typeof(int)] = DbType.Int32,
                [typeof(uint)] = DbType.UInt32,
                [typeof(long)] = DbType.Int64,
                [typeof(ulong)] = DbType.UInt64,
                [typeof(float)] = DbType.Single,
                [typeof(double)] = DbType.Double,
                [typeof(decimal)] = DbType.Decimal,
                [typeof(bool)] = DbType.Boolean,
                [typeof(string)] = DbType.String,
                [typeof(char)] = DbType.StringFixedLength,
                [typeof(Guid)] = DbType.Guid,
                [typeof(DateTime)] = DbType.DateTime,
                [typeof(DateTimeOffset)] = DbType.DateTimeOffset,
                [typeof(TimeSpan)] = DbType.Time,
                [typeof(byte[])] = DbType.Binary,
                [typeof(byte?)] = DbType.Byte,
                [typeof(sbyte?)] = DbType.SByte,
                [typeof(short?)] = DbType.Int16,
                [typeof(ushort?)] = DbType.UInt16,
                [typeof(int?)] = DbType.Int32,
                [typeof(uint?)] = DbType.UInt32,
                [typeof(long?)] = DbType.Int64,
                [typeof(ulong?)] = DbType.UInt64,
                [typeof(float?)] = DbType.Single,
                [typeof(double?)] = DbType.Double,
                [typeof(decimal?)] = DbType.Decimal,
                [typeof(bool?)] = DbType.Boolean,
                [typeof(char?)] = DbType.StringFixedLength,
                [typeof(Guid?)] = DbType.Guid,
                [typeof(DateTime?)] = DbType.DateTime,
                [typeof(DateTimeOffset?)] = DbType.DateTimeOffset,
                [typeof(TimeSpan?)] = DbType.Time,
                [typeof(object)] = DbType.Object
            };

        /// <summary>
        /// Gets the <see cref="TableSchema"/> for the specified entityType and dialect.
        /// </summary>
        public static TableSchema GetTableSchema(Type entityType, IDialect dialect, TableSchemaFactory schemaFactory)
        {
            var key = new TableSchemaCacheIdentity(entityType, dialect.Name);

            if (Schemas.TryGetValue(key, out var result))
            {
                return result;
            }

            var schema = schemaFactory.MakeTableSchema(entityType, dialect);
            Schemas[key] = schema;
            return schema;
        }

        /// <summary>
        /// Gets the <see cref="ConditionColumnSchema"/>s for the specified conditionsType and dialect.
        /// </summary>
        public static ImmutableArray<ConditionColumnSchema> GetConditionsSchema(
            Type entityType,
            TableSchema tableSchema,
            Type conditionsType,
            IDialect dialect,
            TableSchemaFactory schemaFactory)
        {
            var key = new ConditionsColumnCacheIdentity(conditionsType, entityType, dialect.Name);

            if (ConditionColumns.TryGetValue(key, out var result))
            {
                return result;
            }

            var column = schemaFactory.MakeConditionsSchema(conditionsType, tableSchema);
            ConditionColumns[key] = column;
            return column;
        }

        /// <summary>
        /// Create the table schema for an entity
        /// </summary>
        public TableSchema MakeTableSchema(Type entityType, IDialect dialect)
        {
            var tableName = this.tableNameFactory.GetTableName(entityType, dialect);
            var properties = entityType.GetProperties(BindingFlags.Instance | BindingFlags.Public)
                                       .Where(CouldBeColumn)
                                       .Select(PropertySchema.MakePropertySchema)
                                       .Where(p => p.FindAttribute<NotMappedAttribute>() == null)
                                       .ToList();

            var explicitKeyDefined = properties.Any(p => p.FindAttribute<KeyAttribute>() != null);

            var columns = properties.Select(p => this.MakeColumnSchema(dialect, p, GetColumnUsage(explicitKeyDefined, p)));

            return new TableSchema(tableName, columns.ToImmutableArray());
        }

        /// <summary>
        /// Creates the <see cref="ConditionColumnSchema"/> for the <paramref name="conditionsType"/>.
        /// </summary>
        public ImmutableArray<ConditionColumnSchema> MakeConditionsSchema(Type conditionsType, TableSchema tableSchema)
        {
            return conditionsType.GetProperties(BindingFlags.Instance | BindingFlags.Public)
                                 .Where(p => CouldBeColumn(p) && p.GetCustomAttribute<NotMappedAttribute>() == null && p.CanRead)
                                 .Select(p => MakeConditionSchema(tableSchema, p))
                                 .ToImmutableArray();
        }

        /// <summary>
        /// Creates a new <see cref="TableSchemaFactory"/> which generates table names with the <paramref name="factory"/>.
        /// </summary>
        public TableSchemaFactory WithTableNameFactory(ITableNameFactory factory)
        {
            return new TableSchemaFactory(factory, this.columnNameFactory);
        }

        /// <summary>
        /// Creates a new <see cref="TableSchemaFactory"/> which generates column names with the <paramref name="factory"/>.
        /// </summary>
        public TableSchemaFactory WithColumnNameFactory(IColumnNameFactory factory)
        {
            return new TableSchemaFactory(this.tableNameFactory, factory);
        }

        private static bool CouldBeColumn(PropertyInfo property)
        {
            if (property.GetIndexParameters().Length != 0)
            {
                return false;
            }

            var propertyType = property.PropertyType.GetUnderlyingType();
            return propertyType.GetTypeInfo().IsEnum || TypeMapping.ContainsKey(propertyType);
        }

        private static ColumnUsage GetColumnUsage(bool explicitKeyDefined, PropertySchema property)
        {
            var isPrimaryKey = explicitKeyDefined
                ? property.FindAttribute<KeyAttribute>() != null
                : string.Equals(property.Name, "Id", StringComparison.OrdinalIgnoreCase);

            if (!property.PropertyInfo.CanWrite)
            {
                return isPrimaryKey
                    ? ColumnUsage.ComputedPrimaryKey
                    : ColumnUsage.ComputedColumn;
            }

            var generatedAttribute = property.FindAttribute<DatabaseGeneratedAttribute>();
            return isPrimaryKey
                ? GetPrimaryKeyUsage(generatedAttribute)
                : GetColumnUsage(generatedAttribute);
        }

        private static ColumnUsage GetColumnUsage(DatabaseGeneratedAttribute generatedAttribute)
        {
            if (generatedAttribute == null)
            {
                return ColumnUsage.Column;
            }

            switch (generatedAttribute.DatabaseGeneratedOption)
            {
                case DatabaseGeneratedOption.None:
                    return ColumnUsage.Column;
                case DatabaseGeneratedOption.Identity:
                    return ColumnUsage.GeneratedColumn;
                case DatabaseGeneratedOption.Computed:
                    return ColumnUsage.ComputedColumn;
                default:
                    throw new ArgumentOutOfRangeException(
                        "Unknown DatabaseGeneratedOption: " + generatedAttribute.DatabaseGeneratedOption);
            }
        }

        private static ColumnUsage GetPrimaryKeyUsage(DatabaseGeneratedAttribute generatedAttribute)
        {
            if (generatedAttribute == null)
            {
                return ColumnUsage.ComputedPrimaryKey;
            }

            switch (generatedAttribute.DatabaseGeneratedOption)
            {
                case DatabaseGeneratedOption.None:
                    return ColumnUsage.NotGeneratedPrimaryKey;
                case DatabaseGeneratedOption.Identity:
                case DatabaseGeneratedOption.Computed:
                    return ColumnUsage.ComputedPrimaryKey;
                default:
                    throw new ArgumentOutOfRangeException(
                        "Unknown DatabaseGeneratedOption: " + generatedAttribute.DatabaseGeneratedOption);
            }
        }

        private static ConditionColumnSchema MakeConditionSchema(TableSchema tableSchema, PropertyInfo property)
        {
            var propertyName = property.Name;
            var possibleColumns = tableSchema.Columns.Where(c => string.Equals(c.ParameterName, propertyName, StringComparison.OrdinalIgnoreCase)).ToList();

            if (possibleColumns.Count > 1)
            {
                possibleColumns = tableSchema.Columns.Where(c => string.Equals(c.ParameterName, propertyName, StringComparison.Ordinal)).ToList();

                if (possibleColumns.Count > 1)
                {
                    throw new InvalidConditionSchemaException($"Ambiguous property '{propertyName}' on table {tableSchema.Name}");
                }
            }

            if (possibleColumns.Count < 1)
            {
                throw new InvalidConditionSchemaException($"Target table {tableSchema.Name} does not have a property called {propertyName}");
            }

            return new ConditionColumnSchema(possibleColumns.Single(), property);
        }

        private ColumnSchema MakeColumnSchema(IDialect dialect, PropertySchema property, ColumnUsage columnUsage)
        {
            var propertyName = property.Name;

            return new ColumnSchema(
                dialect.MakeColumnName(this.columnNameFactory.GetColumnName(property)),
                dialect.MakeColumnName(propertyName),
                propertyName,
                columnUsage,
                this.GetDbType(property));
        }

        private DbTypeEx GetDbType(PropertySchema property)
        {
            if (property.EffectiveType.GetTypeInfo().IsEnum)
            {
                return new DbTypeEx(DbType.Int32, property.IsNullable, null);
            }

            DbType dbType;
            if (TypeMapping.TryGetValue(property.EffectiveType, out dbType))
            {
                var allowNull = property.IsNullable || (!property.Type.GetTypeInfo().IsValueType && property.FindAttribute<RequiredAttribute>() == null);

                var maxLength = this.GetMaxLength(property);
                return new DbTypeEx(dbType, allowNull, maxLength);
            }

            throw new NotSupportedException("Unknown property type: " + property.EffectiveType);
        }

        private int? GetMaxLength(PropertySchema property)
        {
            if (property.EffectiveType == typeof(char))
            {
                return 1;
            }

            return property.FindAttribute<MaxLengthAttribute>()?.Length;
        }
    }
}