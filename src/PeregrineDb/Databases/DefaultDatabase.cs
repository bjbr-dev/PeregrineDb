namespace PeregrineDb.Databases
{
    using System.Data;

    public class DefaultDatabase
        : DefaultSqlConnection, IDatabase
    {
        /// <summary>
        /// Create a new, dynamic instance of <see cref="DefaultDatabase{T}"/>. This method is a light weight wrapper for generic inference.
        /// </summary>
        public static DefaultDatabase<TConnection> From<TConnection>(TConnection connection, PeregrineConfig config)
            where TConnection : IDbConnection
        {
            return new DefaultDatabase<TConnection>(connection, config);
        }

        public DefaultDatabase(IDbConnection connection, PeregrineConfig config)
            : base(connection, null, config, false)
        {
        }

        public ISqlUnitOfWork StartUnitOfWork(bool leaveOpen = true)
        {
            IDbTransaction transaction = null;
            try
            {
                transaction = this.DbConnection.BeginTransaction();
                return new DefaultUnitOfWork(this.DbConnection, transaction, this.Config, leaveOpen);
            }
            catch
            {
                transaction?.Dispose();
                throw;
            }
        }

        public ISqlUnitOfWork StartUnitOfWork(IsolationLevel isolationLevel, bool leaveOpen = true)
        {
            IDbTransaction transaction = null;
            try
            {
                transaction = this.DbConnection.BeginTransaction(isolationLevel);
                return new DefaultUnitOfWork(this.DbConnection, transaction, this.Config, leaveOpen);
            }
            catch
            {
                transaction?.Dispose();
                throw;
            }
        }
    }
}