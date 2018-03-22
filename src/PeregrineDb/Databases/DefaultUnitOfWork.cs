namespace PeregrineDb.Databases
{
    using System.Data;
    using PeregrineDb.Utils;

    public class DefaultUnitOfWork
        : DefaultDatabaseConnection, IDatabaseUnitOfWork
    {
        /// <summary>
        /// Create a new, dynamic instance of <see cref="DefaultUnitOfWork{TConnection,TTransaction}"/>. This method is a light weight wrapper for generic inference.
        /// </summary>
        public static DefaultUnitOfWork<TConnection, TTransaction> From<TConnection, TTransaction>(
            TConnection connection,
            TTransaction transaction,
            PeregrineConfig config,
            bool leaveOpen)
            where TConnection : IDbConnection
            where TTransaction : IDbTransaction
        {
            return new DefaultUnitOfWork<TConnection, TTransaction>(connection, transaction, config, leaveOpen);
        }

        public DefaultUnitOfWork(IDbConnection connection, IDbTransaction transaction, PeregrineConfig config, bool leaveOpen = false)
            : base(connection, transaction, config, leaveOpen)
        {
            Ensure.NotNull(transaction, nameof(transaction));

            this.Transaction = transaction;
        }

        public IDbTransaction Transaction { get; }

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