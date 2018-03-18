namespace PeregrineDb.Databases
{
    using System.Collections.Generic;
    using Dapper;

    public partial class DefaultDatabaseConnection
    {
        public void CreateTempTable<TEntity>(IEnumerable<TEntity> entities, int? commandTimeout = null)
        {
            this.connection.Execute(this.commandFactory.MakeCreateTempTableCommand<TEntity>(commandTimeout));
            this.connection.Execute(this.commandFactory.MakeInsertRangeCommand(entities, commandTimeout));
        }

        public void DropTempTable<TEntity>(string tableName, int? commandTimeout = null)
        {
            this.DbConnection.Execute(this.commandFactory.MakeDropTempTableCommand<TEntity>(tableName, commandTimeout));
        }
    }
}