// <copyright file="DefaultSqlConnection.cs" company="Berkeleybross">
// Copyright (c) Berkeleybross. All rights reserved.
// </copyright>

namespace PeregrineDb.Databases
{
    using System;
    using System.Data;

    public abstract partial class DefaultSqlConnection
        : ISqlConnection
    {
        private readonly IDbConnection connection;
        private readonly bool leaveOpen;
        private readonly CommandFactory commandFactory;

        private bool disposed;

        protected DefaultSqlConnection(IDbConnection connection, IDbTransaction transaction, PeregrineConfig config, bool leaveOpen)
        {
            this.connection = connection ?? throw new ArgumentNullException(nameof(connection));
            this.Transaction = transaction;
            this.leaveOpen = leaveOpen;
            this.commandFactory = new CommandFactory(config ?? throw new ArgumentNullException(nameof(config)));
        }

        public IDbConnection DbConnection
        {
            get
            {
                this.EnsureNotDisposed();
                return this.connection;
            }
        }

        protected IDbTransaction Transaction { get; }

        public PeregrineConfig Config => this.commandFactory.Config;

        public void Dispose()
        {
            if (this.disposed)
            {
                return;
            }

            try
            {
                this.Dispose(true);
            }
            finally
            {
                if (!this.leaveOpen)
                {
                    this.connection.Dispose();
                }

                this.disposed = true;
            }
        }

        protected virtual void Dispose(bool disposing)
        {
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