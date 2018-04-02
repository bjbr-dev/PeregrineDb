namespace PeregrineDb.Dialects
{
    using System;
    using System.Collections.Immutable;
    using Pagination;
    using PeregrineDb.Schema;

    /// <summary>
    /// Defines the SQL to generate when targeting specific vendor implementations.
    /// </summary>
    public interface IDialect
    {
        /// <summary>
        /// Generates a SQL Select statement which counts how many rows match the <paramref name="conditions"/>.
        /// The statement should return an Int32 Scalar.
        /// </summary>
        SqlCommand MakeCountCommand(TableSchema schema, FormattableString conditions);

        /// <summary>
        /// Generates a SQL statement to select a single row from a table.
        /// </summary>
        SqlCommand MakeFindCommand(TableSchema schema, object id);

        /// <summary>
        /// Generates a SQL statement to select the top N records which match the conditions
        /// </summary>
        SqlCommand MakeGetTopNCommand(TableSchema schema, int take, FormattableString conditions, string orderBy);

        /// <summary>
        /// Generates a SQL statement to select multiple rows.
        /// </summary>
        SqlCommand MakeGetRangeCommand(TableSchema tableSchema, FormattableString conditions);

        /// <summary>
        /// Generates a SQL statement to select a page of rows, in a specific order
        /// </summary>
        SqlCommand MakeGetPageCommand(TableSchema tableSchema, Page page, FormattableString conditions, string orderBy);

        /// <summary>
        /// Generates a SQL statement to insert a row and return the generated identity.
        /// </summary>
        SqlCommand MakeInsertCommand(TableSchema tableSchema, object entity);

        /// <summary>
        /// Generates a SQL statement to insert a row and return the generated identity.
        /// </summary>
        SqlCommand MakeInsertReturningIdentityCommand(TableSchema tableSchema, object entity);

        /// <summary>
        /// Generates a SQL Update statement which chooses which row to update by its PrimaryKey.
        /// </summary>
        SqlCommand MakeUpdateCommand(TableSchema tableSchema, object entity);

        /// <summary>
        /// Generates a SQL Delete statement which chooses which row to delete its PrimaryKey.
        /// </summary>
        SqlCommand MakeDeleteEntityCommand(TableSchema tableSchema, object entity);

        /// <summary>
        /// Generates a SQL Delete statement which chooses which row to delete its PrimaryKey.
        /// </summary>
        SqlCommand MakeDeleteByPrimaryKeyCommand(TableSchema schema, object id);

        /// <summary>
        /// Generates a SQL Delete statement which chooses which row to delete by the <paramref name="conditions"/>.
        /// </summary>
        SqlCommand MakeDeleteRangeCommand(TableSchema tableSchema, FormattableString conditions);

        /// <summary>
        /// Generates a SQL WHERE clause which selects an entity where all the columns match the values in the conditions object.
        /// </summary>
        FormattableString MakeWhereClause(ImmutableArray<ConditionColumnSchema> conditionsSchema, object conditions);

        /// <summary>
        /// Generates a SQL statement which creates a temporary table.
        /// </summary>
        SqlCommand MakeCreateTempTableCommand(TableSchema tableSchema);

        /// <summary>
        /// Generates a SQL statement which drops a temporary table.
        /// </summary>
        SqlCommand MakeDropTempTableCommand(TableSchema tableSchema);
    }
}