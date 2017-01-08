// <copyright file="DbTypeEx.cs" company="Berkeleybross">
// Copyright (c) Berkeleybross. All rights reserved.
// </copyright>
namespace Dapper.MicroCRUD.Schema
{
    using System.Data;

    /// <summary>
    /// Wraps <see cref="DbType"/> and adds extra properties
    /// </summary>
    public class DbTypeEx
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="DbTypeEx"/> class.
        /// </summary>
        public DbTypeEx(DbType type, bool allowNull, int? maxLength)
        {
            this.Type = type;
            this.AllowNull = allowNull;
            this.MaxLength = maxLength;
        }

        /// <summary>
        /// Gets the type of the column
        /// </summary>
        public DbType Type { get; }

        /// <summary>
        /// Gets a value indicating whether the type allows nulls
        /// </summary>
        public bool AllowNull { get; }

        /// <summary>
        /// Gets the maximum length of the column, if it has a length.
        /// </summary>
        public int? MaxLength { get; }
    }
}