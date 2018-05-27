namespace PeregrineDb.Schema
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
    using PeregrineDb.Databases.Mapper;
    using PeregrineDb.Utils;

    /// <summary>
    /// Methods to create an instance of a <see cref="TableSchema"/>.
    /// </summary>
    public class TableSchemaFactory
    {
        private readonly ConcurrentDictionary<TableSchemaCacheIdentity, TableSchema> schemas =
            new ConcurrentDictionary<TableSchemaCacheIdentity, TableSchema>();

        private readonly ConcurrentDictionary<ConditionsColumnCacheIdentity, ImmutableArray<ConditionColumnSchema>> conditionColumns =
            new ConcurrentDictionary<ConditionsColumnCacheIdentity, ImmutableArray<ConditionColumnSchema>>();

        private readonly ISqlNameEscaper nameEscaper;
        private readonly ITableNameConvention tableNameConvention;
        private readonly IColumnNameConvention columnNameConvention;

        /// <summary>
        /// Initializes a new instance of the <see cref="TableSchemaFactory"/> class.
        /// </summary>
        public TableSchemaFactory(
            ISqlNameEscaper nameEscaper,
            ITableNameConvention tableNameConvention,
            IColumnNameConvention columnNameConvention)
        {
            Ensure.NotNull(nameEscaper, nameof(nameEscaper));
            Ensure.NotNull(tableNameConvention, nameof(tableNameConvention));
            Ensure.NotNull(columnNameConvention, nameof(columnNameConvention));

            this.nameEscaper = nameEscaper;
            this.tableNameConvention = tableNameConvention;
            this.columnNameConvention = columnNameConvention;
        }

        /// <summary>
        /// Gets the <see cref="TableSchema"/> for the specified entityType and dialect.
        /// </summary>
        public TableSchema GetTableSchema(Type entityType)
        {
            var key = new TableSchemaCacheIdentity(entityType);

            if (this.schemas.TryGetValue(key, out var result))
            {
                return result;
            }

            var schema = this.MakeTableSchema(entityType);
            this.schemas[key] = schema;
            return schema;
        }

        /// <summary>
        /// Gets the <see cref="ConditionColumnSchema"/>s for the specified conditionsType and dialect.
        /// </summary>
        public ImmutableArray<ConditionColumnSchema> GetConditionsSchema(
            Type entityType,
            TableSchema tableSchema,
            Type conditionsType)
        {
            var key = new ConditionsColumnCacheIdentity(conditionsType, entityType);

            if (this.conditionColumns.TryGetValue(key, out var result))
            {
                return result;
            }

            var column = this.MakeConditionsSchema(conditionsType, tableSchema);
            this.conditionColumns[key] = column;
            return column;
        }

        /// <summary>
        /// Create the table schema for an entity
        /// </summary>
        public TableSchema MakeTableSchema(Type entityType)
        {
            var tableName = this.tableNameConvention.GetTableName(entityType);
            var properties = entityType.GetProperties(BindingFlags.Instance | BindingFlags.Public)
                                       .Where(this.CouldBeColumn)
                                       .Select(PropertySchema.MakePropertySchema)
                                       .Where(p => p.FindAttribute<NotMappedAttribute>() == null)
                                       .ToList();

            var explicitKeyDefined = properties.Any(p => p.FindAttribute<KeyAttribute>() != null);

            var columns = properties.Select((p, i) => this.MakeColumnSchema(i, p, GetColumnUsage(explicitKeyDefined, p)));

            return new TableSchema(tableName, columns.ToImmutableArray());
        }

        /// <summary>
        /// Creates the <see cref="ConditionColumnSchema"/> for the <paramref name="conditionsType"/>.
        /// </summary>
        private ImmutableArray<ConditionColumnSchema> MakeConditionsSchema(Type conditionsType, TableSchema tableSchema)
        {
            return conditionsType.GetProperties(BindingFlags.Instance | BindingFlags.Public)
                                 .Where(p => this.CouldBeColumn(p) && p.GetCustomAttribute<NotMappedAttribute>() == null && p.CanRead)
                                 .Select(p => MakeConditionSchema(tableSchema, p))
                                 .ToImmutableArray();
        }

        private bool CouldBeColumn(PropertyInfo property)
        {
            return property.GetIndexParameters().Length == 0;
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
            var possibleColumns = tableSchema.Columns.Where(c => string.Equals(c.PropertyName, propertyName, StringComparison.OrdinalIgnoreCase)).ToList();

            if (possibleColumns.Count > 1)
            {
                possibleColumns = tableSchema.Columns.Where(c => string.Equals(c.PropertyName, propertyName, StringComparison.Ordinal)).ToList();

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

        private ColumnSchema MakeColumnSchema(int index, PropertySchema property, ColumnUsage columnUsage)
        {
            var propertyName = property.Name;

            return new ColumnSchema(
                index,
                propertyName,
                this.columnNameConvention.GetColumnName(property),
                this.nameEscaper.EscapeColumnName(propertyName),
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

            var dbType = TypeProvider.LookupDbType(property.EffectiveType, property.Name, true, out _);
            var allowNull = property.IsNullable || (!property.Type.GetTypeInfo().IsValueType && property.FindAttribute<RequiredAttribute>() == null);
            return new DbTypeEx(dbType, allowNull, this.GetMaxLength(property));
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