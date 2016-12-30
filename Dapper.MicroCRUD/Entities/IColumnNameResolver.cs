// <copyright file="IColumnNameResolver.cs" company="Berkeleybross">
// Copyright (c) Berkeleybross. All rights reserved.
// </copyright>
namespace Dapper.MicroCRUD.Entities
{
    using System.Reflection;

    /// <summary>
    /// Defines a way to translate an entities property into a database column name
    /// </summary>
    public interface IColumnNameResolver
    {
        /// <summary>
        /// Gets the database column name for the given entity property.
        /// </summary>
        string ResolveColumnName(PropertyInfo propertyInfo, Dialect dialect);
    }
}