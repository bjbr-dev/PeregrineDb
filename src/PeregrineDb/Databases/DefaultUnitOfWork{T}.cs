namespace PeregrineDb.Databases
{
    using System.Data;
    using PeregrineDb.Utils;

    public class DefaultUnitOfWork<TConnection, TTransaction>
        : DefaultSqlConnection, ISqlUnitOfWork<TConnection, TTransaction>
        where TConnection : IDbConnection
        where TTransaction : class, IDbTransaction
    {
        public DefaultUnitOfWork(TConnection connection, TTransaction transaction, PeregrineConfig config, bool leaveOpen = false)
            : base(connection, transaction, config, leaveOpen)
        {
            Ensure.NotNull(transaction, nameof(transaction));
            
            this.DbConnection = connection;
            this.Transaction = transaction;
        }

        public new TConnection DbConnection { get; }

        public TTransaction Transaction { get; }

        public void SaveChanges()
        {
            this.Transaction.Commit();
        }

        public void Rollback()
        {
            this.Transaction.Rollback();
        }

        protected override void Dispose(bool disposing)
        {
            this.Transaction.Dispose();
        }
    }
}