// <copyright file="TableSchemaFactory.cs" company="Berkeleybross">
// Copyright (c) Berkeleybross. All rights reserved.
// </copyright>
namespace Dapper.MicroCRUD.Entities
{
    using System;
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
        /// Create the table schema for an entity
        /// </summary>
        public static TableSchema GetTableSchema(Type entityType, Dialect dialect)
        {
            var columnNameResolver = MicroCRUDConfig.ColumnNameResolver;
            var tableNameResolver = MicroCRUDConfig.TableNameResolver;

            var tableName = tableNameResolver.ResolveTableName(entityType, dialect);
            var properties = entityType.GetProperties().Where(p =>
            {
                var propertyType = p.PropertyType.GetUnderlyingType();
                return propertyType.IsEnum || PossiblePropertyTypes.Contains(propertyType);
            }).ToList();

            var explicitKeyDefined = properties.Any(p => p.GetCustomAttribute<KeyAttribute>() != null);

            var columns = properties.Select(p => new ColumnSchema(
                columnNameResolver.ResolveColumnName(p, dialect),
                dialect.EscapeMostReservedCharacters(p.Name),
                p.Name,
                GetColumnUsage(explicitKeyDefined, p)));

            return new TableSchema(tableName, columns.ToImmutableList());
        }

        private static ColumnUsage GetColumnUsage(bool explicitKeyDefined, PropertyInfo property)
        {
            var isPrimaryKey = explicitKeyDefined
                ? property.GetCustomAttribute<KeyAttribute>() != null
                : string.Equals(property.Name, "Id", StringComparison.OrdinalIgnoreCase);

            var generatedAttribute = property.GetCustomAttribute<DatabaseGeneratedAttribute>();
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
                return ColumnUsage.PrimaryKey;
            }

            switch (generatedAttribute.DatabaseGeneratedOption)
            {
                case DatabaseGeneratedOption.None:
                    return ColumnUsage.NotGeneratedPrimaryKey;
                case DatabaseGeneratedOption.Identity:
                case DatabaseGeneratedOption.Computed:
                    return ColumnUsage.PrimaryKey;
                default:
                    throw new ArgumentOutOfRangeException(
                        "Unknown DatabaseGeneratedOption: " + generatedAttribute.DatabaseGeneratedOption);
            }
        }
    }
}