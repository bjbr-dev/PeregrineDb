// <copyright file="DefaultDatabase{T}.cs" company="Berkeleybross">
// Copyright (c) Berkeleybross. All rights reserved.
// </copyright>

namespace PeregrineDb.Databases
{
    using System.Data;

    public class DefaultDatabase<TConnection>
        : DefaultSqlConnection, IDatabase<TConnection>
        where TConnection : IDbConnection
    {
        public DefaultDatabase(TConnection connection, PeregrineConfig config, bool leaveOpen = false)
            : base(connection, null, config, leaveOpen)
        {
            this.DbConnection = connection;
        }

        public new TConnection DbConnection { get; }

        public ISqlUnitOfWork<TConnection, IDbTransaction> StartUnitOfWork(bool leaveOpen = true)
        {
            IDbTransaction transaction = null;
            try
            {
                transaction = this.DbConnection.BeginTransaction();
                return DefaultUnitOfWork.From(this.DbConnection, transaction, this.Config, leaveOpen);
            }
            catch
            {
                transaction?.Dispose();
                throw;
            }
        }

        public ISqlUnitOfWork<TConnection, IDbTransaction> StartUnitOfWork(IsolationLevel isolationLevel, bool leaveOpen = true)
        {
            IDbTransaction transaction = null;
            try
            {
                transaction = this.DbConnection.BeginTransaction(isolationLevel);
                return DefaultUnitOfWork.From(this.DbConnection, transaction, this.Config, leaveOpen);
            }
            catch
            {
                transaction?.Dispose();
                throw;
            }
        }

        public ISqlUnitOfWork<TConnection, TTransaction> StartUnitOfWork<TTransaction>(bool leaveOpen = true)
            where TTransaction : class, IDbTransaction
        {
            IDbTransaction transaction = null;
            try
            {
                transaction = this.DbConnection.BeginTransaction();
                return DefaultUnitOfWork.From(this.DbConnection, (TTransaction)transaction, this.Config, leaveOpen);
            }
            catch
            {
                transaction?.Dispose();
                throw;
            }
        }

        public ISqlUnitOfWork<TConnection, TTransaction> StartUnitOfWork<TTransaction>(IsolationLevel isolationLevel, bool leaveOpen = true)
            where TTransaction : class, IDbTransaction
        {
            IDbTransaction transaction = null;
            try
            {
                transaction = this.DbConnection.BeginTransaction(isolationLevel);
                return DefaultUnitOfWork.From(this.DbConnection, (TTransaction)transaction, this.Config, leaveOpen);
            }
            catch
            {
                transaction?.Dispose();
                throw;
            }
        }
    }
}