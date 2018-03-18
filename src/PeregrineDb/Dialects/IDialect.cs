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
        /// Gets the name of the dialect
        /// </summary>
        string Name { get; }

        /// <summary>
        /// Generates a SQL Select statement which counts how many rows match the <paramref name="conditions"/>.
        /// </summary>
        string MakeCountStatement(TableSchema tableSchema, string conditions);

        /// <summary>
        /// Generates a SQL statement to select a single row from a table.
        /// </summary>
        string MakeFindStatement(TableSchema tableSchema);

        /// <summary>
        /// Generates a SQL statement to select the top N records which match the conditions
        /// </summary>
        string MakeGetTopNStatement(TableSchema tableSchema, int take, string conditions, string orderBy);

        /// <summary>
        /// Generates a SQL statement to select multiple rows.
        /// </summary>
        string MakeGetRangeStatement(TableSchema tableSchema, string conditions);

        /// <summary>
        /// Generates a SQL statement to select a page of rows, in a specific order
        /// </summary>
        string MakeGetPageStatement(TableSchema tableSchema, Page page, string conditions, string orderBy);

        /// <summary>
        /// Generates a SQL statement to insert a row and return the generated identity.
        /// </summary>
        string MakeInsertStatement(TableSchema tableSchema);

        /// <summary>
        /// Generates a SQL statement to insert a row and return the generated identity.
        /// </summary>
        string MakeInsertReturningIdentityStatement(TableSchema tableSchema);

        /// <summary>
        /// Generates a SQL Update statement which chooses which row to update by its PrimaryKey.
        /// </summary>
        string MakeUpdateStatement(TableSchema tableSchema);

        /// <summary>
        /// Generates a SQL Delete statement which chooses which row to delete its PrimaryKey.
        /// </summary>
        string MakeDeleteByPrimaryKeyStatement(TableSchema tableSchema);

        /// <summary>
        /// Generates a SQL Delete statement which chooses which row to delete by the <paramref name="conditions"/>.
        /// </summary>
        string MakeDeleteRangeStatement(TableSchema tableSchema, string conditions);

        /// <summary>
        /// Generates a SQL WHERE clause which selects an entity where all the columns match the values in the conditions object.
        /// </summary>
        string MakeWhereClause(ImmutableArray<ConditionColumnSchema> conditionsSchema, object conditions);

        /// <summary>
        /// Generates a SQL statement which creates a temporary table.
        /// </summary>
        string MakeCreateTempTableStatement(TableSchema tableSchema);

        /// <summary>
        /// Generates a SQL statement which drops a temporary table.
        /// </summary>
        string MakeDropTempTableStatement(TableSchema tableSchema);

        /// <summary>
        /// Escapes the column name so it can be used in SQL
        /// </summary>
        string MakeColumnName(string name);

        /// <summary>
        /// Escapes the table name so it can be used in SQL
        /// </summary>
        string MakeTableName(string tableName);

        /// <summary>
        /// Escapes the table and schema names, and then combines them so they can be used in SQL
        /// </summary>
        string MakeTableName(string schema, string tableName);
    }
}