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
        public static TableSchema GetTableSchema(Type entityType, MicroCRUDConfig config)
        {
            var tableName = config.TableNameResolver.ResolveTableName(entityType, config.Dialect);
            var properties = entityType.GetProperties()
                                       .Where(p =>
                                       {
                                           var propertyType = p.PropertyType.GetUnderlyingType();
                                           return propertyType.IsEnum || PossiblePropertyTypes.Contains(propertyType);
                                       })
                                       .Where(p => p.GetCustomAttribute(typeof(NotMappedAttribute)) == null)
                                       .ToList();

            var explicitKeyDefined = properties.Any(p => p.GetCustomAttribute<KeyAttribute>() != null);

            var columns = properties.Select(
                p => config.Dialect.MakeColumnSchema(
                    p.Name,
                    config.ColumnNameResolver.ResolveColumnName(p),
                    GetColumnUsage(explicitKeyDefined, p)));

            return new TableSchema(tableName, columns.ToImmutableArray());
        }

        private static ColumnUsage GetColumnUsage(bool explicitKeyDefined, PropertyInfo property)
        {
            var isPrimaryKey = explicitKeyDefined
                ? property.GetCustomAttribute<KeyAttribute>() != null
                : string.Equals(property.Name, "Id", StringComparison.OrdinalIgnoreCase);

            if (!property.CanWrite)
            {
                return isPrimaryKey
                    ? ColumnUsage.ComputedPrimaryKey
                    : ColumnUsage.ComputedColumn;
            }

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
    }
}