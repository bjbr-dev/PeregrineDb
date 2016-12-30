// <copyright file="TableSchema.cs" company="Berkeleybross">
//   Copyright (c) Berkeleybross. All rights reserved.
// </copyright>
namespace Dapper.MicroCRUD.Entities
{
    using System;
    using System.Collections.Generic;
    using System.Collections.Immutable;
    using System.Linq;

    /// <summary>
    /// Represents a table in the database, as derived from the definition of an entity.
    /// </summary>
    public class TableSchema
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="TableSchema"/> class.
        /// </summary>
        /// <param name="name"></param>
        /// <param name="columns"></param>
        public TableSchema(string name, ImmutableList<ColumnSchema> columns)
        {
            this.Name = name;
            this.Columns = columns;
        }

        /// <summary>
        /// Gets the name of the database table.
        /// </summary>
        public string Name { get; }

        /// <summary>
        /// Gets the columns which form the Primary Key of this table.
        /// </summary>
        public IReadOnlyList<ColumnSchema> PrimaryKeyColumns => this.Columns.Where(c => c.IsPrimaryKey).ToList();

        /// <summary>
        /// Gets the columns in the table.
        /// </summary>
        public ImmutableList<ColumnSchema> Columns { get; }

        /// <summary>
        /// Gets the column which is the Primary Key of this table.
        /// Throws an exception if there is no key, or if there are multiple columns.
        /// </summary>
        /// <exception cref="ArgumentException">
        /// This table doesn't have a primary key, or there are multiple keys.
        /// Nb: should be an <see cref="InvalidOperationException"/> but this is a conveniance method for validation and it's more important that the end user sees an appropriate exception.
        /// </exception>
        public ColumnSchema GetSinglePrimaryKey(string callerName)
        {
            var result = this.GetPrimaryKeys(callerName);

            if (result.Count > 1)
            {
                throw new ArgumentException(callerName + " only supports an entity with a single [Key] or Id property");
            }

            return result[0];
        }

        /// <summary>
        /// Gets the columns which form the Primary Key of this table.
        /// Throws an exception if there is no key.
        /// </summary>
        /// <exception cref="ArgumentException">
        /// This table doesn't have a primary key.
        /// Nb: should be an <see cref="InvalidOperationException"/> but this is a conveniance method for validation and it's more important that the end user sees an appropriate exception.
        /// </exception>
        public IReadOnlyList<ColumnSchema> GetPrimaryKeys(string callerName)
        {
            var result = this.PrimaryKeyColumns;
            if (result.Count == 0)
            {
                throw new ArgumentException(callerName + " only supports an entity with a [Key] or Id property");
            }

            return result;
        }
    }
}