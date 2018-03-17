namespace Dapper.MicroCRUD.Databases
{
    using System;
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

        public static DefaultDatabase Create<T>(T seed, Func<T, IDbConnection> connectionFactory, IDialect dialect)
        {
            var connection = connectionFactory(seed);
            try
            {
                return new DefaultDatabase(connection, dialect);
            }
            catch
            {
                connection.Dispose();
                throw;
            }
        }

        public IDbConnection DbConnection { get; }

        public IDbTransaction Transaction => null;

        public IDialect Dialect { get; }

        public IUnitOfWork StartUnitOfWork()
        {
            var transaction = this.DbConnection.BeginTransaction();
            return DefaultUnitOfWorkFactory.Create(this, transaction, false);
        }

        public IUnitOfWork StartUnitOfWork(IsolationLevel isolationLevel)
        {
            var transaction = this.DbConnection.BeginTransaction(isolationLevel);
            return DefaultUnitOfWorkFactory.Create(this, transaction, false);
        }

        public void Dispose()
        {
            this.DbConnection.Dispose();
        }
    }
}