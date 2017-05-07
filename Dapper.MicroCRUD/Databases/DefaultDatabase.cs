namespace Dapper.MicroCRUD.Databases
{
    using System.Data;
    using Dapper.MicroCRUD.Dialects;

    public class DefaultDatabase
        : IDatabase
    {
        public DefaultDatabase(IDbConnection connection, IDialect dialect)
        {
            this.DbConnection = connection;
            this.Dialect = dialect;
        }

        public IDbConnection DbConnection { get; }

        public IDbTransaction Transaction => null;

        public IDialect Dialect { get; }

        public IUnitOfWork StartUnitOfWork()
        {
            var transaction = this.DbConnection.BeginTransaction();
            return DefaultUnitOfWork.Create(this, transaction, false);
        }

        public IUnitOfWork StartUnitOfWork(IsolationLevel isolationLevel)
        {
            var transaction = this.DbConnection.BeginTransaction(isolationLevel);
            return DefaultUnitOfWork.Create(this, transaction, false);
        }

        public void Dispose()
        {
            this.DbConnection.Dispose();
        }
    }
}