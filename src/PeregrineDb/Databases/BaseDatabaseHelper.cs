namespace Dapper.MicroCRUD.Databases
{
    using System;
    using System.Data;
    using Dapper.MicroCRUD.Dialects;

    public abstract class BaseDatabaseHelper
        : IUnitOfWork
    {
        protected BaseDatabaseHelper(IDapperConnection database)
        {
            this.Database = database;
        }

        public IDbConnection DbConnection => this.Database.DbConnection;

        public IDbTransaction Transaction => this.Database.Transaction;

        public IDialect Dialect => this.Database.Dialect;

        protected IDapperConnection Database { get; }

        public void Dispose()
        {
            var disposable = this.Database as IDisposable;
            disposable?.Dispose();
        }

        public void SaveChanges()
        {
            var unitOfWork = this.Database as IUnitOfWork;
            if (unitOfWork == null)
            {
                throw new InvalidOperationException("This helper does not have an active unit of work");
            }

            unitOfWork.SaveChanges();
        }

        public void Rollback()
        {
            var unitOfWork = this.Database as IUnitOfWork;
            if (unitOfWork == null)
            {
                throw new InvalidOperationException("This helper does not have an active unit of work");
            }

            unitOfWork.Rollback();
        }
    }
}