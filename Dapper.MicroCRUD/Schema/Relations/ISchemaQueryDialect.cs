namespace Dapper.MicroCRUD.Schema.Relations
{
    using Dapper.MicroCRUD.Dialects;

    public interface ISchemaQueryDialect
        : IDialect
    {
        /// <summary>
        /// Generates a SQL SELECT statement which returns the name of all tables in the current database.
        /// The result should have a single string field called 'Name', which is the name of a table (including its schema).
        /// </summary>
        string MakeGetAllTablesStatement();

        /// <summary>
        /// Generates a SQL SELECT statement which returns the relations between two tables in the current database.
        /// The result should have the following fields:
        /// - ReferencedTable, System.String: The name of the table being referenced by a foreign key.
        /// - ReferencingTable, System.String: The name of the table containing the foreign key.
        /// - ReferencingColumn, System.String: The name of the column with the foreign key.
        /// - RelationIsOptional, System.Boolean: Whether the column is nullable (and therefore whether the relation is optional).
        /// </summary>
        string MakeGetAllRelationsStatement();

        /// <summary>
        /// Generates a SQL UPDATE statement which sets a specified column to NULL for all rows in the table.
        /// </summary>
        string MakeSetColumnNullStatement(string tableName, string columnName);
    }
}