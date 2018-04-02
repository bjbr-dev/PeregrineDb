namespace PeregrineDb.Schema
{
    public interface ISqlNameEscaper
    {
        /// <summary>
        /// Escapes the column name so it can be used in SQL
        /// </summary>
        string EscapeColumnName(string name);

        /// <summary>
        /// Escapes the table name so it can be used in SQL
        /// </summary>
        string EscapeTableName(string tableName);

        /// <summary>
        /// Escapes the table and schema names, and then combines them so they can be used in SQL
        /// </summary>
        string EscapeTableName(string schema, string tableName);
    }
}