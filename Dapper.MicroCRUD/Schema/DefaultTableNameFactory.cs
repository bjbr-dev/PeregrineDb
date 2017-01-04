// <copyright file="DefaultTableNameFactory.cs" company="Berkeleybross">
// Copyright (c) Berkeleybross. All rights reserved.
// </copyright>
namespace Dapper.MicroCRUD.Schema
{
    using System;
    using System.ComponentModel.DataAnnotations.Schema;
    using System.Reflection;

    /// <summary>
    /// Default implementation of an <see cref="ITableNameFactory"/>.
    /// Uses the <see cref="TableAttribute"/> if present, otherwise takes the class name and pluralizes it.
    /// </summary>
    public class DefaultTableNameFactory
        : ITableNameFactory
    {
        /// <inheritdoc/>
        public string GetTableName(Type type, Dialect dialect)
        {
            var tableAttribute = type.GetCustomAttribute<TableAttribute>(false);
            return tableAttribute != null
                ? GetTableNameFromAttribute(dialect, tableAttribute)
                : dialect.EscapeMostReservedCharacters(this.GetTableNameFromType(type));
        }

        /// <summary>
        /// Gets the table name from the given type.
        /// By default, pluralizes and removes the interface "I" prefix.
        /// </summary>
        protected virtual string GetTableNameFromType(Type type)
        {
            return type.Name + "s";
        }

        private static string GetTableNameFromAttribute(Dialect dialect, TableAttribute tableAttribute)
        {
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