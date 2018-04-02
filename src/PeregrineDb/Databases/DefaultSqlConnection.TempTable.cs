namespace PeregrineDb.Databases
{
    using System.Collections.Generic;

    public partial class DefaultSqlConnection
    {
        public void CreateTempTable<TEntity>(IEnumerable<TEntity> entities, int? commandTimeout = null)
        {
            var createCommand = this.commandFactory.MakeCreateTempTableCommand<TEntity>();
            this.Execute(in createCommand, commandTimeout);
            this.InsertRange(entities, commandTimeout);
        }

        public void DropTempTable<TEntity>(string tableName, int? commandTimeout = null)
        {
            var command = this.commandFactory.MakeDropTempTableCommand<TEntity>(tableName);
            this.Execute(in command, commandTimeout);
        }
    }
}