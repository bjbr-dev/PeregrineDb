// <copyright file="DefaultColumnNameFactory.cs" company="Berkeleybross">
// Copyright (c) Berkeleybross. All rights reserved.
// </copyright>
namespace Dapper.MicroCRUD.Schema
{
    using System.ComponentModel.DataAnnotations.Schema;

    /// <summary>
    /// Default implementation of a column name factory.
    /// Uses the <see cref="TableAttribute"/> if present, otherwise pluralizes class name.
    /// </summary>
    public class DefaultColumnNameFactory
        : IColumnNameFactory
    {
        /// <inheritdoc />
        public string GetColumnName(PropertySchema property)
        {
            var columnAttribute = property.FindAttribute<ColumnAttribute>();
            return columnAttribute != null
                ? columnAttribute.Name
                : property.Name;
        }
    }
}