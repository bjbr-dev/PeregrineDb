namespace PeregrineDb.Databases
{
    using System.Data;

    public class DefaultDatabase<TConnection>
        : DefaultDatabase, IDatabase<TConnection>
        where TConnection : IDbConnection
    {
        public DefaultDatabase(TConnection connection, PeregrineConfig config)
            : base(connection, config)
        {
            this.DbConnection = connection;
        }

        public new TConnection DbConnection { get; }

        public IDatabaseUnitOfWork<TConnection, TTransaction> StartUnitOfWork<TTransaction>(bool leaveOpen = true)
            where TTransaction : class, IDbTransaction
        {
            TTransaction transaction = null;
            try
            {
                transaction = (TTransaction)this.DbConnection.BeginTransaction();
                return DefaultUnitOfWork.From(this.DbConnection, transaction, this.Config, leaveOpen);
            }
            catch
            {
                transaction?.Dispose();
                throw;
            }
        }

        public IDatabaseUnitOfWork<TConnection, TTransaction> StartUnitOfWork<TTransaction>(IsolationLevel isolationLevel, bool leaveOpen = true)
            where TTransaction : class, IDbTransaction
        {
            TTransaction transaction = null;
            try
            {
                transaction = (TTransaction)this.DbConnection.BeginTransaction(isolationLevel);
                return DefaultUnitOfWork.From(this.DbConnection, transaction, this.Config, leaveOpen);
            }
            catch
            {
                transaction?.Dispose();
                throw;
            }
        }
    }
}