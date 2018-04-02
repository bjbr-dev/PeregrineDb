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
        FormattableString MakeCountStatement(TableSchema schema, FormattableString conditions);

        /// <summary>
        /// Generates a SQL statement to select a single row from a table.
        /// </summary>
        FormattableString MakeFindStatement(TableSchema schema, object id);

        /// <summary>
        /// Generates a SQL statement to select the top N records which match the conditions
        /// </summary>
        FormattableString MakeGetTopNStatement(TableSchema schema, int take, FormattableString conditions, string orderBy);

        /// <summary>
        /// Generates a SQL statement to select multiple rows.
        /// </summary>
        FormattableString MakeGetRangeStatement(TableSchema tableSchema, FormattableString conditions);

        /// <summary>
        /// Generates a SQL statement to select a page of rows, in a specific order
        /// </summary>
        FormattableString MakeGetPageStatement(TableSchema tableSchema, Page page, FormattableString conditions, string orderBy);

        /// <summary>
        /// Generates a SQL statement to insert a row and return the generated identity.
        /// </summary>
        FormattableString MakeInsertStatement(TableSchema tableSchema, object entity);

        /// <summary>
        /// Generates a SQL statement to insert a row and return the generated identity.
        /// </summary>
        FormattableString MakeInsertReturningIdentityStatement(TableSchema tableSchema, object entity);

        /// <summary>
        /// Generates a SQL Update statement which chooses which row to update by its PrimaryKey.
        /// </summary>
        FormattableString MakeUpdateStatement(TableSchema tableSchema, object entity);

        /// <summary>
        /// Generates a SQL Delete statement which chooses which row to delete its PrimaryKey.
        /// </summary>
        FormattableString MakeDeleteEntityStatement(TableSchema tableSchema, object entity);

        /// <summary>
        /// Generates a SQL Delete statement which chooses which row to delete its PrimaryKey.
        /// </summary>
        FormattableString MakeDeleteByPrimaryKeyStatement(TableSchema schema, object id);

        /// <summary>
        /// Generates a SQL Delete statement which chooses which row to delete by the <paramref name="conditions"/>.
        /// </summary>
        FormattableString MakeDeleteRangeStatement(TableSchema tableSchema, FormattableString conditions);

        /// <summary>
        /// Generates a SQL WHERE clause which selects an entity where all the columns match the values in the conditions object.
        /// </summary>
        FormattableString MakeWhereClause(ImmutableArray<ConditionColumnSchema> conditionsSchema, object conditions);

        /// <summary>
        /// Generates a SQL statement which creates a temporary table.
        /// </summary>
        FormattableString MakeCreateTempTableStatement(TableSchema tableSchema);

        /// <summary>
        /// Generates a SQL statement which drops a temporary table.
        /// </summary>
        FormattableString MakeDropTempTableStatement(TableSchema tableSchema);

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