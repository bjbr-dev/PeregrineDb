// <copyright file="AtttributeTableNameConvention.cs" company="Berkeleybross">
// Copyright (c) Berkeleybross. All rights reserved.
// </copyright>

namespace PeregrineDb.Schema
{
    using System;
    using System.ComponentModel.DataAnnotations.Schema;
    using System.Reflection;

    /// <summary>
    /// If class has <see cref="TableAttribute"/> then it returns <see cref="TableAttribute.Name"/> without manipulating it. Otherwise;
    /// - Removes the specified suffix, if any (so classes can be called e.g. DogEntity)
    /// - Converts the class name into snake_case
    /// </summary>
    public class AtttributeTableNameConvention
        : ITableNameConvention
    {
        protected const string DefaultSuffix = "Entity";
        private readonly string suffix;

        public AtttributeTableNameConvention(ISqlNameEscaper nameEscaper, string suffix = DefaultSuffix)
        {
            this.NameEscaper = nameEscaper;
            this.suffix = suffix;
        }

        public ISqlNameEscaper NameEscaper { get; }

        /// <inheritdoc/>
        public string GetTableName(Type type)
        {
            var tableAttribute = type.GetTypeInfo().GetCustomAttribute<TableAttribute>(false);
            return tableAttribute != null
                ? GetTableNameFromAttribute(tableAttribute, this.NameEscaper)
                : this.GetTableNameFromType(type);
        }

        /// <summary>
        /// Gets the table name from the given type.
        /// By default, pluralizes and removes the interface "I" prefix.
        /// </summary>
        protected virtual string GetTableNameFromType(Type type)
        {
            return this.NameEscaper.EscapeTableName(type.Name + "s");
        }

        private static string GetTableNameFromAttribute(TableAttribute tableAttribute, ISqlNameEscaper nameEscaper)
        {
            return string.IsNullOrEmpty(tableAttribute.Schema)
                ? nameEscaper.EscapeTableName(tableAttribute.Name)
                : nameEscaper.EscapeTableName(tableAttribute.Schema, tableAttribute.Name);
        }

        protected string RemoveSuffix(string value)
        {
            if (this.suffix == null)
            {
                return value;
            }

            var length = value.Length - this.suffix.Length;
            if (length <= 0)
            {
                return value;
            }

            return value.EndsWith(this.suffix, StringComparison.OrdinalIgnoreCase)
                ? value.Substring(0, length)
                : value;
        }
    }
}