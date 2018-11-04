// <copyright file="DefaultDatabase.cs" company="Berkeleybross">
// Copyright (c) Berkeleybross. All rights reserved.
// </copyright>
namespace PeregrineDb.Databases
{
    using System.Data;

    public class DefaultDatabase
        : DefaultSqlConnection, IDatabase
    {
        public DefaultDatabase(IDbConnection connection, PeregrineConfig config, bool leaveOpen = false)
            : base(connection, null, config, leaveOpen)
        {
        }

        public ISqlUnitOfWork StartUnitOfWork(bool leaveOpen = true)
        {
            IDbTransaction transaction = null;
            try
            {
                transaction = this.DbConnection.BeginTransaction();
                return new DefaultUnitOfWork(this.DbConnection, transaction, this.Config, leaveOpen);
            }
            catch
            {
                transaction?.Dispose();
                throw;
            }
        }

        public ISqlUnitOfWork StartUnitOfWork(IsolationLevel isolationLevel, bool leaveOpen = true)
        {
            IDbTransaction transaction = null;
            try
            {
                transaction = this.DbConnection.BeginTransaction(isolationLevel);
                return new DefaultUnitOfWork(this.DbConnection, transaction, this.Config, leaveOpen);
            }
            catch
            {
                transaction?.Dispose();
                throw;
            }
        }
    }
}