// <copyright file="TableSchemaFactory.cs" company="Berkeleybross">
// Copyright (c) Berkeleybross. All rights reserved.
// </copyright>
namespace Dapper.MicroCRUD.Entities
{
    using System;
    using System.Collections.Generic;
    using System.Collections.Immutable;
    using System.ComponentModel.DataAnnotations;
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

            var columns = properties.Select(p =>
            {
                var isPrimaryKey = explicitKeyDefined
                    ? p.GetCustomAttribute<KeyAttribute>() != null
                    : string.Equals(p.Name, "Id", StringComparison.OrdinalIgnoreCase);

                return new ColumnSchema(
                    columnNameResolver.ResolveColumnName(p, dialect),
                    dialect.EscapeMostReservedCharacters(p.Name),
                    p.Name,
                    isPrimaryKey,
                    isPrimaryKey && p.GetCustomAttribute<RequiredAttribute>() == null);
            });

            return new TableSchema(tableName, columns.ToImmutableList());
        }
    }
}