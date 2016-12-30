// <copyright file="ITableNameResolver.cs" company="Berkeleybross">
// Copyright (c) Berkeleybross. All rights reserved.
// </copyright>
namespace Dapper.MicroCRUD.Entities
{
    using System;

    /// <summary>
    /// Defines a way to translate an entity type into a database table name
    /// </summary>
    public interface ITableNameResolver
    {
        /// <summary>
        /// Gets the database table name for the given entity type.
        /// </summary>
        string ResolveTableName(Type type, Dialect dialect);
    }
}