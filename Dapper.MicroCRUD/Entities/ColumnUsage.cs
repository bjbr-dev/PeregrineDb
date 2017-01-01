﻿// <copyright file="ColumnUsage.cs" company="Berkeleybross">
// Copyright (c) Berkeleybross. All rights reserved.
// </copyright>
namespace Dapper.MicroCRUD.Entities
{
    /// <summary>
    /// Summarizes when a <see cref="ColumnSchema"/> should be included in various conditions.
    /// </summary>
    public class ColumnUsage
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="ColumnUsage"/> class.
        /// </summary>
        public ColumnUsage(
            bool isPrimaryKey,
            bool includeInInsertStatements,
            bool includeInUpdateStatements)
        {
            this.IncludeInInsertStatements = includeInInsertStatements;
            this.IncludeInUpdateStatements = includeInUpdateStatements;
            this.IsPrimaryKey = isPrimaryKey;
        }

        /// <summary>
        /// Gets a <see cref="ColumnUsage"/> for a normal, non-primary key column.
        /// </summary>
        public static ColumnUsage Column { get; } = new ColumnUsage(false, true, true);

        /// <summary>
        /// Gets a <see cref="ColumnUsage"/> for a column with a value which is generated by the database when a row is created, e.g. a created timestamp.
        /// It will be included in SELECT and UPDATE statements, but not INSERT statements.
        /// </summary>
        public static ColumnUsage GeneratedColumn { get; } = new ColumnUsage(false, false, true);

        /// <summary>
        /// Gets a <see cref="ColumnUsage"/> for a column with a value which is computed by the database when a row is created or updated, e.g. a last update timestamp.
        /// It will be included in SELECT statements, but not INSERT or UPDATE statements.
        /// </summary>
        public static ColumnUsage ComputedColumn { get; } = new ColumnUsage(false, false, false);

        /// <summary>
        /// Gets a <see cref="ColumnUsage"/> for a primary key with a value which is generated by the database.
        /// It will not be included in INSERT or UPDATE statements.
        /// </summary>
        public static ColumnUsage ComputedPrimaryKey { get; } = new ColumnUsage(true, false, false);

        /// <summary>
        /// Gets a <see cref="ColumnUsage"/> for a primary key with a value which is generated by the database.
        /// It will be included in INSERT statements but not UPDATE statements.
        /// </summary>
        public static ColumnUsage NotGeneratedPrimaryKey { get; } = new ColumnUsage(true, true, false);

        /// <summary>
        /// Gets a value indicating whether the property is (part of) the primary key.
        /// </summary>
        public bool IsPrimaryKey { get; }

        /// <summary>
        /// Gets a value indicating whether the property should be included in INSERT statements.
        /// </summary>
        public bool IncludeInInsertStatements { get; }

        /// <summary>
        /// Gets a value indicating whether the property should be included in UPDATE statements.
        /// </summary>
        public bool IncludeInUpdateStatements { get; }
    }
}