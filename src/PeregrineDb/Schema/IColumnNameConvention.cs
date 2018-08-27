// <copyright file="IColumnNameConvention.cs" company="Berkeleybross">
// Copyright (c) Berkeleybross. All rights reserved.
// </copyright>

namespace PeregrineDb.Schema
{
    /// <summary>
    /// Defines how to get the column name from a specific property.
    /// </summary>
    public interface IColumnNameConvention
    {
        /// <summary>
        /// Gets the escaped name of the column from the <paramref name="property"/>.
        /// </summary>
        string GetColumnName(PropertySchema property);
    }
}