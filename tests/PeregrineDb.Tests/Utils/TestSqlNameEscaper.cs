namespace PeregrineDb.Tests.Utils
{
    using PeregrineDb.Schema;

    internal class TestSqlNameEscaper
        : ISqlNameEscaper
    {
        /// <inheritdoc />
        public string EscapeColumnName(string name)
        {
            return "'" + name + "'";
        }

        /// <inheritdoc />
        public string EscapeTableName(string tableName)
        {
            return "'" + tableName + "'";
        }

        /// <inheritdoc />
        public string EscapeTableName(string schema, string tableName)
        {
            return "'" + schema + "'.'" + tableName + "'";
        }
    }
}
