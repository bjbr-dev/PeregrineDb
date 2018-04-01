namespace PeregrineDb.Databases
{
    using System.Collections.Generic;

    public partial class DefaultDatabaseConnection
    {
        public void CreateTempTable<TEntity>(IEnumerable<TEntity> entities, int? commandTimeout = null)
        {
            this.Execute(this.commandFactory.MakeCreateTempTableStatement<TEntity>(), commandTimeout);
            this.InsertRange(entities, commandTimeout);
        }

        public void DropTempTable<TEntity>(string tableName, int? commandTimeout = null)
        {
            this.Execute(this.commandFactory.MakeDropTempTableStatement<TEntity>(tableName), commandTimeout);
        }
    }
}