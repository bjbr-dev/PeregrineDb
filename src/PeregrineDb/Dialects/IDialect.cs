// <copyright file="IDialect.cs" company="Berkeleybross">
// Copyright (c) Berkeleybross. All rights reserved.
// </copyright>

namespace PeregrineDb.Dialects
{
    using System.Collections.Generic;
    using System.Collections.Immutable;
    using Pagination;
    using PeregrineDb.Schema;

    /// <summary>
    /// Defines the SQL to generate when targeting specific vendor implementations.
    /// </summary>
    public interface IDialect
    {
        /// <summary>
        /// Creates a command which counts how many rows match the <paramref name="conditions"/>.
        /// The statement should return an Int32 Scalar.
        /// </summary>
        SqlCommand MakeCountCommand(string conditions, object parameters, TableSchema tableSchema);

        /// <summary>
        /// Generates a SQL statement to select a single row from a table.
        /// </summary>
        SqlCommand MakeFindCommand(object id, TableSchema tableSchema);

        /// <summary>
        /// Generates a SQL statement to select the first N records which match the <paramref name="conditions"/>.
        /// </summary>
        SqlCommand MakeGetFirstNCommand(int take, string conditions, object parameters, string orderBy, TableSchema tableSchema);

        /// <summary>
        /// Generates a SQL statement to select multiple rows.
        /// </summary>
        SqlCommand MakeGetRangeCommand(string conditions, object parameters, TableSchema tableSchema);

        /// <summary>
        /// Generates a SQL statement to select a page of rows, in a specific order.
        /// </summary>
        SqlCommand MakeGetPageCommand(Page page, string conditions, object parameters, string orderBy, TableSchema tableSchema);

        /// <summary>
        /// Generates a SQL statement to insert a row with no return value.
        /// </summary>
        SqlCommand MakeInsertCommand(object entity, TableSchema tableSchema);

        /// <summary>
        /// Generates a SQL statement to insert a row and return the generated identity.
        /// </summary>
        SqlCommand MakeInsertReturningPrimaryKeyCommand(object entity, TableSchema tableSchema);

        SqlMultipleCommand<TEntity> MakeInsertRangeCommand<TEntity>(IEnumerable<TEntity> entities, TableSchema tableSchema);

        /// <summary>
        /// Generates a SQL Update statement which chooses which row to update by its PrimaryKey.
        /// </summary>
        SqlCommand MakeUpdateCommand(object entity, TableSchema tableSchema);

        SqlMultipleCommand<TEntity> MakeUpdateRangeCommand<TEntity>(IEnumerable<TEntity> entities, TableSchema tableSchema)
            where TEntity : class;

        /// <summary>
        /// Generates a SQL Delete statement which chooses which row to delete its PrimaryKey.
        /// </summary>
        SqlCommand MakeDeleteCommand(object entity, TableSchema tableSchema);

        /// <summary>
        /// Generates a SQL Delete statement which chooses which row to delete by its PrimaryKey.
        /// </summary>
        SqlCommand MakeDeleteByPrimaryKeyCommand(object id, TableSchema tableSchema);

        /// <summary>
        /// Generates a SQL Delete statement which chooses which row to delete by the <paramref name="conditions"/>.
        /// </summary>
        SqlCommand MakeDeleteRangeCommand(string conditions, object parameters, TableSchema tableSchema);

        SqlCommand MakeDeleteAllCommand(TableSchema tableSchema);

        /// <summary>
        /// Generates a SQL statement which creates a temporary table.
        /// </summary>
        /// <param name="tableSchema"></param>
        SqlCommand MakeCreateTempTableCommand(TableSchema tableSchema);

        /// <summary>
        /// Generates a SQL statement which drops a temporary table.
        /// </summary>
        /// <param name="tableSchema"></param>
        SqlCommand MakeDropTempTableCommand(TableSchema tableSchema);

        string MakeWhereClause(ImmutableArray<ConditionColumnSchema> conditionsSchema, object conditions);
    }
}