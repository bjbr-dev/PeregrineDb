namespace Dapper.MicroCRUD.Databases
{
    using System;
    using System.Data;
    using Dapper.MicroCRUD.Utils;

    public class DefaultUnitOfWorkFactory
    {
        /// <summary>
        /// Creates a <see cref="DefaultUnitOfWork"/> with a <see cref="DefaultDatabase"/>.
        /// The connection created by <paramref name="databaseFactory"/> will be opened so that the transaction can be begun.
        /// </summary>
        public static IUnitOfWork Create<T>(T seed, Func<T, IDatabase> databaseFactory)
        {
            return Create(seed, databaseFactory, c => c.BeginTransaction());
        }

        /// <summary>
        /// Creates a <see cref="DefaultUnitOfWork"/> with a <see cref="DefaultDatabase"/>.
        /// The connection created by <paramref name="databaseFactory"/> will be opened so that the transaction can be begun.
        /// </summary>
        public static IUnitOfWork Create<T>(T seed, Func<T, IDatabase> databaseFactory, IsolationLevel isolationLevel)
        {
            return Create(seed, databaseFactory, c => c.BeginTransaction(isolationLevel));
        }

        private static IUnitOfWork Create<T>(T seed, Func<T, IDatabase> databaseFactory, Func<IDbConnection, IDbTransaction> transactionFactory)
        {
            Ensure.NotNull(databaseFactory, nameof(databaseFactory));

            var database = databaseFactory(seed);
            if (database == null)
            {
                throw new ArgumentException("Database factory returned null");
            }

            try
            {
                if (database.DbConnection.State == ConnectionState.Closed)
                {
                    database.DbConnection.Open();
                }

                return Create(database, transactionFactory(database.DbConnection));
            }
            catch
            {
                database.Dispose();
                throw;
            }
        }

        /// <summary>
        /// Creates a <see cref="DefaultUnitOfWork"/>.
        /// Throws an exception if database is null, without disposing of transaction (because transaction is supposed to belong to database).
        /// Otherwise if an exception happens, it will safely dispose of the transaction, and optionally the database
        /// </summary>
        public static IUnitOfWork Create(IDatabase database, IDbTransaction transaction, bool disposeDatabase = true)
        {
            Ensure.NotNull(database, nameof(database));

            if (transaction == null)
            {
                if (disposeDatabase)
                {
                    database.Dispose();
                }

                throw new ArgumentNullException(nameof(transaction));
            }

            try
            {
                return new DefaultUnitOfWork(database, transaction, disposeDatabase);
            }
            catch (Exception ex)
            {
                try
                {
                    try
                    {
                        transaction.Rollback();
                    }
                    catch (Exception rollbackEx)
                    {
                        throw new AggregateException(ex, rollbackEx);
                    }
                    finally
                    {
                        transaction.Dispose();
                    }
                }
                finally
                {
                    if (disposeDatabase)
                    {
                        database.Dispose();
                    }
                }

                throw;
            }
        }
    }
}