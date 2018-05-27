namespace PeregrineDb.Databases
{
    using System.Collections.Generic;
    using System.Data;

    public partial class DefaultSqlConnection
    {
        public void CreateTempTable<TEntity>(IEnumerable<TEntity> entities, int? commandTimeout = null)
        {
            var command = this.Dialect.MakeCreateTempTableCommand<TEntity>();
            this.Execute(command.CommandText, command.Parameters, CommandType.Text, commandTimeout);
            this.InsertRange(entities, commandTimeout);
        }

        public void DropTempTable<TEntity>(int? commandTimeout = null)
        {
            var command = this.Dialect.MakeDropTempTableCommand<TEntity>();
            this.Execute(command.CommandText, command.Parameters, CommandType.Text, commandTimeout);
        }
    }
}