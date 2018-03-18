namespace PeregrineDb.Databases
{
    using System.Data;

    public class DefaultUnitOfWork
        : DefaultDatabaseConnection, IDatabaseUnitOfWork
    {
        private readonly AutoRollbackTransaction transaction;

        public DefaultUnitOfWork(IDbConnection connection, IDbTransaction transaction, PeregrineConfig config, bool disposeConnection = true)
            : base(connection, transaction, config, disposeConnection)
        {
            this.transaction = new AutoRollbackTransaction(transaction);
        }

        public IDbTransaction Transaction => this.transaction.Transaction;

        public void SaveChanges()
        {
            this.transaction.SaveChanges();
        }

        public void Rollback()
        {
            this.transaction.Rollback();
        }

        protected override void Dispose(bool disposing)
        {
            this.transaction.Dispose();
        }
    }
}