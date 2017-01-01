// <copyright file="TableSchema.cs" company="Berkeleybross">
//   Copyright (c) Berkeleybross. All rights reserved.
// </copyright>
namespace Dapper.MicroCRUD.Entities
{
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
        /// Gets the column which is the Primary Key of this table.
        /// Throws an exception if there is no key, or if there are multiple columns.
        /// </summary>
        /// <exception cref="InvalidPrimaryKeyException">This table doesn't have a primary key, or there are multiple keys.</exception>
        public ColumnSchema GetSinglePrimaryKey()
        {
            var result = this.GetPrimaryKeys();

            if (result.Count > 1)
            {
                throw new InvalidPrimaryKeyException(
                    "This method only supports an entity with a single [Key] or Id property");
            }

            return result[0];
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