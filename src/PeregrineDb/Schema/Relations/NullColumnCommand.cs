namespace PeregrineDb.Schema.Relations
{
    internal class NullColumnCommand
    {
        public NullColumnCommand(string tableName, string columnName)
        {
            this.TableName = tableName;
            this.ColumnName = columnName;
        }

        public string TableName { get; }

        public string ColumnName { get; }
    }
}