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

        public IDatabaseUnitOfWork StartUnitOfWork(bool leaveOpen = true)
        {
            IDbTransaction transaction = null;
            try
            {
                transaction = this.DbConnection.BeginTransaction();
                return new DefaultUnitOfWork(this.DbConnection, transaction, this.Config, !leaveOpen);
            }
            catch
            {
                transaction?.Dispose();
                throw;
            }
        }

        public IDatabaseUnitOfWork StartUnitOfWork(IsolationLevel isolationLevel, bool leaveOpen = true)
        {
            IDbTransaction transaction = null;
            try
            {
                transaction = this.DbConnection.BeginTransaction(isolationLevel);
                return new DefaultUnitOfWork(this.DbConnection, transaction, this.Config, !leaveOpen);
            }
            catch
            {
                transaction?.Dispose();
                throw;
            }
        }
    }
}