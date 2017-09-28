namespace Dapper.MicroCRUD.Schema.Relations
{
    internal class ClearTableCommand
    {
        public ClearTableCommand(string tableName)
        {
            this.TableName = tableName;
        }

        public string TableName { get;  }
    }
}