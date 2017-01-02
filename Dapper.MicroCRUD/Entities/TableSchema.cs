// <copyright file="TableSchema.cs" company="Berkeleybross">
//   Copyright (c) Berkeleybross. All rights reserved.
// </copyright>
namespace Dapper.MicroCRUD.Entities
{
    using System;
    using System.Collections.Generic;
    using System.Collections.Immutable;
    using System.Linq;
    using Dapper.MicroCRUD.Utils;

    /// <summary>
    /// Represents a table in the database, as derived from the definition of an entity.
    /// </summary>
    public class TableSchema
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="TableSchema"/> class.
        /// </summary>
        public TableSchema(string name, IReadOnlyList<ColumnSchema> columns)
        {
            this.Name = name;
            this.Columns = columns;
            this.PrimaryKeyColumns = columns.Where(c => c.Usage.IsPrimaryKey).ToImmutableArray();
        }

        /// <summary>
        /// Gets the name of the database table.
        /// </summary>
        public string Name { get; }

        /// <summary>
        /// Gets the columns which form the Primary Key of this table.
        /// </summary>
        public IReadOnlyList<ColumnSchema> PrimaryKeyColumns { get; }

        /// <summary>
        /// Gets the columns in the table.
        /// </summary>
        public IReadOnlyList<ColumnSchema> Columns { get; }

        /// <summary>
        /// Gets whether this table can generate a primary key with the specified type.
        /// </summary>
        public bool CanGeneratePrimaryKey(Type type)
        {
            if (this.PrimaryKeyColumns.Count != 1)
            {
                return false;
            }

            var keyType = type.GetUnderlyingType();
            return keyType == typeof(int) || keyType == typeof(long);
        }

        /// <summary>
        /// Gets the columns which form the Primary Key of this table.
        /// Throws an exception if there is no key.
        /// </summary>
        /// <exception cref="InvalidPrimaryKeyException">This table doesn't have a primary key.</exception>
        public IReadOnlyList<ColumnSchema> GetPrimaryKeys()
        {
            var result = this.PrimaryKeyColumns;
            if (result.Count == 0)
            {
                throw new InvalidPrimaryKeyException("This method only supports an entity with a [Key] or Id property");
            }

            return result;
        }
    }
}