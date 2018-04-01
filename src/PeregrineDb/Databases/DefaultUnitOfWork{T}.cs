namespace PeregrineDb.Databases
{
    using System.Data;

    public class DefaultUnitOfWork<TConnection, TTransaction>
        : DefaultUnitOfWork, ISqlUnitOfWork<TConnection, TTransaction>
        where TConnection : IDbConnection
        where TTransaction : IDbTransaction
    {
        public DefaultUnitOfWork(TConnection connection, TTransaction transaction, PeregrineConfig config, bool leaveOpen = false)
            : base(connection, transaction, config, leaveOpen)
        {
            this.DbConnection = connection;
            this.Transaction = transaction;
        }

        public new TConnection DbConnection { get; }

        public new TTransaction Transaction { get; }
    }
}