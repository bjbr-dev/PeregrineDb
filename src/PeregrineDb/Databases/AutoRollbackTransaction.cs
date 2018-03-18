namespace PeregrineDb.Databases
{
    using System;
    using System.Data;
    using PeregrineDb.Utils;

    public class AutoRollbackTransaction
        : IDisposable
    {
        private bool completed;

        public AutoRollbackTransaction(IDbTransaction transaction)
        {
            Ensure.NotNull(transaction, nameof(transaction));
            this.Transaction = transaction;
        }

        public IDbTransaction Transaction { get; }

        public void SaveChanges()
        {
            this.Transaction.Commit();
            this.completed = true;
        }

        public void Rollback()
        {
            this.Transaction.Rollback();
            this.completed = true;
        }

        public void Dispose()
        {
            try
            {
                if (!this.completed)
                {
                    this.Transaction.Rollback();
                    this.completed = true;
                }
            }
            finally
            {
                this.Transaction.Dispose();
            }
        }
    }
}