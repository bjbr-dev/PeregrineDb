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
    using System.Linq;
    using System.Reflection;
    using Dapper.MicroCRUD.Utils;

    /// <summary>
    /// Methods to create an instance of a <see cref="TableSchema"/>.
    /// </summary>
    internal static class TableSchemaFactory
    {
        private static readonly ConcurrentDictionary<TableSchemaCacheIdentity, TableSchema> Schemas =
            new ConcurrentDictionary<TableSchemaCacheIdentity, TableSchema>();

        private static readonly List<Type> PossiblePropertyTypes = new List<Type>
            {
                typeof(byte),
                typeof(sbyte),
                typeof(short),
                typeof(ushort),
                typeof(int),
                typeof(uint),
                typeof(long),
                typeof(ulong),
                typeof(float),
                typeof(double),
                typeof(decimal),
                typeof(bool),
                typeof(string),
                typeof(char),
                typeof(Guid),
                typeof(DateTime),
                typeof(DateTimeOffset),
                typeof(byte[])
            };

        /// <summary>
        /// Gets the <see cref="TableSchema"/> for the specified entityType and dialect.
        /// </summary>
        public static TableSchema GetTableSchema(Type entityType, Dialect dialect)
        {
            dialect = dialect ?? MicroCRUDConfig.DefaultDialect;

            var key = new TableSchemaCacheIdentity(entityType, dialect.Name);

            TableSchema result;
            if (Schemas.TryGetValue(key, out result))
            {
                return result;
            }

            var schema = MakeTableSchema(entityType, dialect);
            Schemas[key] = schema;
            return schema;
        }

        /// <summary>
        /// Create the table schema for an entity
        /// </summary>
        internal static TableSchema MakeTableSchema(Type entityType, Dialect dialect)
        {
            var tableName = ResolveTableName(entityType, dialect);
            var properties = entityType.GetProperties()
                                       .Where(p =>
                                       {
                                           var propertyType = p.PropertyType.GetUnderlyingType();
                                           return propertyType.IsEnum || PossiblePropertyTypes.Contains(propertyType);
                                       })
                                       .Select(
                                           p => new PropertySchema
                                               {
                                                   CustomAttributes = p.GetCustomAttributes(false),
                                                   Name = p.Name,
                                                   PropertyInfo = p
                                               })
                                       .Where(p => p.FindAttribute<NotMappedAttribute>() == null)
                                       .ToList();

            var explicitKeyDefined = properties.Any(p => p.FindAttribute<KeyAttribute>() != null);

            var columns = properties.Select(
                p => dialect.MakeColumnSchema(
                    p.Name,
                    ResolveColumnName(p),
                    GetColumnUsage(explicitKeyDefined, p)));

            return new TableSchema(tableName, columns.ToImmutableArray());
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

        private static string ResolveTableName(Type type, Dialect dialect)
        {
            var tableAttribute = type.GetCustomAttribute<TableAttribute>(false);
            if (tableAttribute == null)
            {
                return dialect.EscapeMostReservedCharacters(type.Name);
            }

            var tableName = dialect.EscapeMostReservedCharacters(tableAttribute.Name);
            if (string.IsNullOrEmpty(tableAttribute.Schema))
            {
                return tableName;
            }

            var schemaName = dialect.EscapeMostReservedCharacters(tableAttribute.Schema);
            return $"{schemaName}.{tableName}";
        }

        private static string ResolveColumnName(PropertySchema p)
        {
            var columnAttribute = p.CustomAttributes.OfType<ColumnAttribute>().FirstOrDefault();
            return columnAttribute != null
                ? columnAttribute.Name
                : p.Name;
        }
    }
}