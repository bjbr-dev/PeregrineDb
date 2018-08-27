// <copyright file="AttributeColumnNameConvention.cs" company="Berkeleybross">
// Copyright (c) Berkeleybross. All rights reserved.
// </copyright>

namespace PeregrineDb.Schema
{
    using System.ComponentModel.DataAnnotations.Schema;

    /// <summary>
    /// Default implementation of a column name convention.
    /// Uses the <see cref="ColumnAttribute"/> if present, otherwise uses the property name.
    /// </summary>
    public class AttributeColumnNameConvention
        : IColumnNameConvention
    {
        public AttributeColumnNameConvention(ISqlNameEscaper nameEscaper)
        {
            this.NameEscaper = nameEscaper;
        }

        public ISqlNameEscaper NameEscaper { get; }

        /// <inheritdoc />
        public string GetColumnName(PropertySchema property)
        {
            var columnAttribute = property.FindAttribute<ColumnAttribute>();
            return columnAttribute != null
                ? this.NameEscaper.EscapeColumnName(columnAttribute.Name)
                : this.GetColumnNameFromType(property);
        }

        /// <summary>
        /// Gets the column name from the given property.
        /// </summary>
        protected virtual string GetColumnNameFromType(PropertySchema property)
        {
            return this.NameEscaper.EscapeColumnName(property.Name);
        }
    }
}