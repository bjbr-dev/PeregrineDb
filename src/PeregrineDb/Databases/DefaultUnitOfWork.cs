namespace PeregrineDb.Databases
{
    using System;
    using System.Data;
    using PeregrineDb.Utils;

    public class DefaultUnitOfWork
        : DefaultDatabaseConnection, IDatabaseUnitOfWork
    {
        private bool completed;
        private readonly IDbTransaction transaction;

        public DefaultUnitOfWork(IDbConnection connection, IDbTransaction transaction, PeregrineConfig config, bool disposeConnection = true)
            : base(connection, transaction, config, disposeConnection)
        {
            Ensure.NotNull(transaction, nameof(transaction));
            this.transaction = transaction;
        }

        public IDbTransaction Transaction
        {
            get
            {
                if (this.transaction != null)
                {
                    return this.transaction;
                }
                
                throw new InvalidOperationException("No transaction has been started");
            }
        }

        public void SaveChanges()
        {
            if (this.transaction != null)
            {
                this.transaction.Commit();
                this.completed = true;
            }
            else
            {
                throw new InvalidOperationException("No transaction to save");
            }
        }

        public void Rollback()
        {
            if (this.transaction != null)
            {
                this.transaction.Rollback();
                this.completed = true;
            }
            else
            {
                throw new InvalidOperationException("No transaction to rollback");
            }
        }

        protected override void Dispose(bool disposing)
        {
            try
            {
                if (!this.completed)
                {
                    this.transaction?.Rollback();
                    this.completed = true;
                }
            }
            finally
            {
                this.transaction?.Dispose();
            }
        }
    }
}