// <copyright file="AutoRollbackTransaction.cs" company="Berkeleybross">
// Copyright (c) Berkeleybross. All rights reserved.
// </copyright>

namespace PeregrineDb.Databases
{
    using System.Data;
    using PeregrineDb.Utils;

    public class AutoRollbackTransaction
        : IDbTransaction
    {
        private bool completed;

        public AutoRollbackTransaction(IDbTransaction transaction)
        {
            Ensure.NotNull(transaction, nameof(transaction));
            this.Transaction = transaction;
        }

        public IDbTransaction Transaction { get; }

        public IDbConnection Connection => this.Transaction.Connection;

        public IsolationLevel IsolationLevel => this.Transaction.IsolationLevel;

        public void Commit()
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