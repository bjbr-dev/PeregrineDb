namespace PeregrineDb.Databases
{
    using System.Collections.Generic;

    public partial class DefaultSqlConnection
    {
        public void CreateTempTable<TEntity>(IEnumerable<TEntity> entities, int? commandTimeout = null)
        {
            var createCommand = this.Dialect.MakeCreateTempTableCommand<TEntity>();
            this.Execute(in createCommand, commandTimeout);
            this.InsertRange(entities, commandTimeout);
        }

        public void DropTempTable<TEntity>(int? commandTimeout = null)
        {
            var command = this.Dialect.MakeDropTempTableCommand<TEntity>();
            this.Execute(in command, commandTimeout);
        }
    }
}