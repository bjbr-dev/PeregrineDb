// <copyright file="IDialect.cs" company="Berkeleybross">
// Copyright (c) Berkeleybross. All rights reserved.
// </copyright>

namespace PeregrineDb.Dialects
{
    using System.Collections.Generic;
    using Pagination;

    /// <summary>
    /// Defines the SQL to generate when targeting specific vendor implementations.
    /// </summary>
    public interface IDialect
    {
        /// <summary>
        /// Creates a command which counts how many rows match the <paramref name="conditions"/>.
        /// The statement should return an Int32 Scalar.
        /// </summary>
        SqlCommand MakeCountCommand<TEntity>(string conditions, object parameters);

        /// <summary>
        /// Creates a command which counts how many rows match the <paramref name="conditions"/>.
        /// The statement should return an Int32 Scalar.
        /// </summary>
        SqlCommand MakeCountCommand<TEntity>(object conditions);

        /// <summary>
        /// Generates a SQL statement to select a single row from a table.
        /// </summary>
        SqlCommand MakeFindCommand<TEntity>(object id);

        /// <summary>
        /// Generates a SQL statement to select the first N records which match the <paramref name="conditions"/>.
        /// </summary>
        SqlCommand MakeGetFirstNCommand<TEntity>(int take, string conditions, object parameters, string orderBy);

        /// <summary>
        /// Generates a SQL statement to select the first N records which match the <paramref name="conditions"/>.
        /// </summary>
        SqlCommand MakeGetFirstNCommand<TEntity>(int take, object conditions, string orderBy);

        /// <summary>
        /// Generates a SQL statement to select multiple rows.
        /// </summary>
        SqlCommand MakeGetRangeCommand<TEntity>(string conditions, object parameters);

        /// <summary>
        /// Generates a SQL statement to select multiple rows.
        /// </summary>
        SqlCommand MakeGetRangeCommand<TEntity>(object conditions);

        /// <summary>
        /// Generates a SQL statement to select a page of rows, in a specific order.
        /// </summary>
        SqlCommand MakeGetPageCommand<TEntity>(Page page, string conditions, object parameters, string orderBy);

        /// <summary>
        /// Generates a SQL statement to select a page of rows, in a specific order.
        /// </summary>
        SqlCommand MakeGetPageCommand<TEntity>(Page page, object conditions, string orderBy);

        /// <summary>
        /// Generates a SQL statement to insert a row and return the generated identity.
        /// </summary>
        SqlCommand MakeInsertCommand(object entity);

        /// <summary>
        /// Generates a SQL statement to insert a row and return the generated identity.
        /// </summary>
        SqlCommand MakeInsertReturningPrimaryKeyCommand<TPrimaryKey>(object entity);

        SqlMultipleCommand<TEntity> MakeInsertRangeCommand<TEntity>(IEnumerable<TEntity> entities);

        /// <summary>
        /// Generates a SQL Update statement which chooses which row to update by its PrimaryKey.
        /// </summary>
        SqlCommand MakeUpdateCommand<TEntity>(TEntity entity)
            where TEntity : class;

        SqlMultipleCommand<TEntity> MakeUpdateRangeCommand<TEntity>(IEnumerable<TEntity> entities)
            where TEntity : class;

        /// <summary>
        /// Generates a SQL Delete statement which chooses which row to delete its PrimaryKey.
        /// </summary>
        SqlCommand MakeDeleteCommand<TEntity>(TEntity entity)
            where TEntity : class;

        /// <summary>
        /// Generates a SQL Delete statement which chooses which row to delete its PrimaryKey.
        /// </summary>
        SqlCommand MakeDeleteByPrimaryKeyCommand<TEntity>(object id);

        /// <summary>
        /// Generates a SQL Delete statement which chooses which row to delete by the <paramref name="conditions"/>.
        /// </summary>
        SqlCommand MakeDeleteRangeCommand<TEntity>(string conditions, object parameters);

        SqlCommand MakeDeleteRangeCommand<TEntity>(object conditions);

        SqlCommand MakeDeleteAllCommand<TEntity>();

        /// <summary>
        /// Generates a SQL statement which creates a temporary table.
        /// </summary>
        SqlCommand MakeCreateTempTableCommand<TEntity>();

        /// <summary>
        /// Generates a SQL statement which drops a temporary table.
        /// </summary>
        SqlCommand MakeDropTempTableCommand<TEntity>();
    }
}