// <copyright file="ITableNameFactory.cs" company="Berkeleybross">
// Copyright (c) Berkeleybross. All rights reserved.
// </copyright>
namespace Dapper.MicroCRUD.Schema
{
    using System;

    /// <summary>
    /// Defines how to get the table name from a specific type.
    /// </summary>
    public interface ITableNameFactory
    {
        /// <summary>
        /// Gets the table name from the <paramref name="type"/>.
        /// The table name should be properly escaped for the given <paramref name="dialect"/>
        /// (Probably by calling <see cref="Dialect.EscapeMostReservedCharacters"/>.
        /// </summary>
        string GetTableName(Type type, Dialect dialect);
    }
}