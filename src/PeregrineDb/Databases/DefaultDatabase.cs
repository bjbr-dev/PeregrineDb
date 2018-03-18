namespace PeregrineDb.Databases
{
    using System.Data;

    public class DefaultDatabase
        : DefaultDatabaseConnection, IDatabase
    {
        public DefaultDatabase(IDbConnection connection, PeregrineConfig config)
            : base(connection, null, config, true)
        {
        }

        public IDatabaseUnitOfWork StartUnitOfWork()
        {
            IDbTransaction transaction = null;
            try
            {
                transaction = this.DbConnection.BeginTransaction();
                return new DefaultUnitOfWork(this.DbConnection, transaction, this.Config, false);
            }
            catch
            {
                transaction?.Dispose();
                throw;
            }
        }

        public IDatabaseUnitOfWork StartUnitOfWork(IsolationLevel isolationLevel)
        {
            IDbTransaction transaction = null;
            try
            {
                transaction = this.DbConnection.BeginTransaction(isolationLevel);
                return new DefaultUnitOfWork(this.DbConnection, transaction, this.Config, false);
            }
            catch
            {
                transaction?.Dispose();
                throw;
            }
        }
    }
}