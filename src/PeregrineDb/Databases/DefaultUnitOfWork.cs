namespace Dapper.MicroCRUD.Databases
{
    using System;
    using System.Data;
    using Dapper.MicroCRUD.Dialects;

    /// <summary>
    /// Represents a single transaction in a database.
    /// </summary>
    /// <example>
    /// <code>
    /// <![CDATA[
    /// using (var database = this.databaseFactory.StartUnitOfWork()) {
    ///     database.Delete<UserEntity>(id);
    ///     database.SaveChanges();
    /// }
    /// ]]>
    /// </code>
    /// </example>
    public sealed class DefaultUnitOfWork
        : IUnitOfWork
    {
        private readonly IDatabase database;
        private readonly IDbTransaction transaction;
        private readonly bool disposeDatabase;
        private bool completed;
        private bool disposed;

        public DefaultUnitOfWork(IDatabase database, IDbTransaction transaction, bool disposeDatabase = true)
        {
            this.database = database ?? throw new ArgumentNullException(nameof(database));
            this.transaction = transaction ?? throw new ArgumentNullException(nameof(transaction));
            this.disposeDatabase = disposeDatabase;
        }

        public IDbConnection DbConnection
        {
            get
            {
                this.EnsureNotDisposed();
                return this.database.DbConnection;
            }
        }

        public IDbTransaction Transaction
        {
            get
            {
                this.EnsureNotDisposed();
                return this.transaction;
            }
        }

        public IDialect Dialect => this.database.Dialect;

        public void SaveChanges()
        {
            this.EnsureNotDisposed();
            this.Transaction.Commit();
            this.completed = true;
        }

        public void Rollback()
        {
            this.EnsureNotDisposed();
            this.Transaction.Rollback();
            this.completed = true;
        }

        public void Dispose()
        {
            if (this.disposed)
            {
                return;
            }

            try
            {
                if (!this.completed)
                {
                    this.transaction.Rollback();
                    this.completed = true;
                }
            }
            finally
            {
                try
                {
                    this.transaction.Dispose();
                }
                finally
                {
                    if (this.disposeDatabase)
                    {
                        this.database.Dispose();
                    }
                }
            }

            this.disposed = true;
        }

        private void EnsureNotDisposed()
        {
            if (this.disposed)
            {
                throw new ObjectDisposedException(this.GetType().FullName);
            }
        }
    }
}