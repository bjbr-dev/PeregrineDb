// <copyright file="DefaultTableNameResolver.cs" company="Berkeleybross">
//   Copyright (c) Berkeleybross. All rights reserved.
// </copyright>
namespace Dapper.MicroCRUD.Entities
{
    using System;
    using System.ComponentModel.DataAnnotations.Schema;
    using System.Reflection;
    using Dapper.MicroCRUD;

    /// <summary>
    /// If the entity has the <see cref="TableAttribute"/> defined then uses that name, otherwise takes the name of the class.
    /// </summary>
    public class DefaultTableNameResolver
        : ITableNameResolver
    {
        /// <inheritdoc/>
        public string ResolveTableName(Type type, Dialect dialect)
        {
            var tableAttribute = type.GetCustomAttribute<TableAttribute>(true);
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
    }
}