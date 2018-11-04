// <copyright file="DefaultUnitOfWork.cs" company="Berkeleybross">
// Copyright (c) Berkeleybross. All rights reserved.
// </copyright>

namespace PeregrineDb.Databases
{
    using System.Data;
    using PeregrineDb.Utils;

    public class DefaultUnitOfWork
        : DefaultSqlConnection, ISqlUnitOfWork
    {
        public DefaultUnitOfWork(IDbConnection connection, IDbTransaction transaction, PeregrineConfig config, bool leaveOpen = false)
            : base(connection, transaction, config, leaveOpen)
        {
            Ensure.NotNull(transaction, nameof(transaction));
        }

        public new IDbTransaction Transaction => base.Transaction;

        public void SaveChanges()
        {
            this.Transaction.Commit();
        }

        public void Rollback()
        {
            this.Transaction.Rollback();
        }

        protected override void Dispose(bool disposing)
        {
            this.Transaction.Dispose();
        }
    }
}