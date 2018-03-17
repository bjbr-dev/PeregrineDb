// <copyright file="DatabaseFixture.cs" company="Berkeleybross">
// Copyright (c) Berkeleybross. All rights reserved.
// </copyright>
namespace Dapper.MicroCRUD.Tests
{
    using System;
    using Dapper.MicroCRUD.Databases;
    using Dapper.MicroCRUD.Dialects;
    using Dapper.MicroCRUD.Tests.Utils;

    public abstract class DatabaseFixture
        : IDisposable
    {
        protected DatabaseFixture(string dialectName)
        {
            this.Database = BlankDatabaseFactory.MakeDatabase(dialectName);
            this.DatabaseDialect = this.Database.Dialect;
            this.DefaultDatabase = new DefaultDatabase(this.Database.Connection, this.DatabaseDialect);
        }

        public BlankDatabase Database { get; }

        public IDialect DatabaseDialect { get; set; }

        public IDatabase DefaultDatabase { get; set; }

        public void Dispose()
        {
            this.Database.Dispose();
        }
    }
}