// <copyright file="BlankDatabase.cs" company="Berkeleybross">
// Copyright (c) Berkeleybross. All rights reserved.
// </copyright>
namespace Dapper.MicroCRUD.Tests.Utils
{
    using System;
    using System.Data;

    internal class BlankDatabase
        : IDisposable
    {
        private readonly Action dropDatabase;
        private bool disposed;

        public BlankDatabase(Dialect dialect, IDbConnection connection, Action dropDatabase)
        {
            this.Connection = connection;
            this.dropDatabase = dropDatabase;
            this.Dialect = dialect;
        }

        public Dialect Dialect { get; }

        public IDbConnection Connection { get; }

        public void Dispose()
        {
            if (this.disposed)
            {
                return;
            }

            this.Connection.Dispose();
            this.dropDatabase();
            this.disposed = true;
        }
    }
}