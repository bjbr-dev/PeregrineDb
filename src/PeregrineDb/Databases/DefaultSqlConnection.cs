// <copyright file="DefaultSqlConnection.cs" company="Berkeleybross">
// Copyright (c) Berkeleybross. All rights reserved.
// </copyright>

namespace PeregrineDb.Databases
{
    using System;
    using System.Collections.Immutable;
    using System.Data;
    using PeregrineDb.Dialects;
    using PeregrineDb.Schema;

    public abstract partial class DefaultSqlConnection
        : ISqlConnection
    {
        private readonly IDbConnection connection;
        private readonly bool leaveOpen;

        private bool disposed;

        protected DefaultSqlConnection(IDbConnection connection, IDbTransaction transaction, PeregrineConfig config, bool leaveOpen)
        {
            this.connection = connection ?? throw new ArgumentNullException(nameof(connection));
            this.Transaction = transaction;
            this.leaveOpen = leaveOpen;
            this.Config = config ?? throw new ArgumentNullException(nameof(config));
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

        public PeregrineConfig Config { get; }

        public IDialect Dialect => this.Config.Dialect;

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

        protected TableSchema GetTableSchema(Type entityType)
        {
            return this.Config.SchemaFactory.GetTableSchema(entityType);
        }

        protected ImmutableArray<ConditionColumnSchema> GetConditionsSchema(
            Type entityType,
            TableSchema tableSchema,
            Type conditionsType)
        {
            return this.Config.SchemaFactory.GetConditionsSchema(entityType, tableSchema, conditionsType);
        }
    }
}