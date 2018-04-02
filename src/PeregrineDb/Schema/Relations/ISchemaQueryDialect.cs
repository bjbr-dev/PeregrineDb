namespace PeregrineDb.Schema.Relations
{
    using PeregrineDb.Dialects;

    public interface ISchemaQueryDialect
        : IDialect
    {
        /// <summary>
        /// Generates a SQL SELECT statement which returns the name of all tables in the current databaseConnection.
        /// The result should have a single string field called 'Name', which is the name of a table (including its schema).
        /// </summary>
        SqlCommand MakeGetAllTablesStatement();

        /// <summary>
        /// Generates a SQL SELECT statement which returns the relations between two tables in the current databaseConnection.
        /// The result should have the following fields:
        /// - TargetTable, System.String: The name of the table being referenced by a foreign key.
        /// - SourceTable, System.String: The name of the table containing the foreign key.
        /// - SourceColumn, System.String: The name of the column with the foreign key.
        /// - SourceIsOptional, System.Boolean: Whether the column is nullable (and therefore whether the relation is optional).
        /// </summary>
        SqlCommand MakeGetAllRelationsStatement();

        /// <summary>
        /// Generates a SQL UPDATE statement which sets a specified column to NULL for all rows in the table.
        /// </summary>
        SqlCommand MakeSetColumnNullStatement(string tableName, string columnName);

        SqlCommand MakeDeleteAllCommand(string tableName);
    }
}