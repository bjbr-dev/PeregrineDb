// <copyright file="DatabaseFixture.cs" company="Berkeleybross">
// Copyright (c) Berkeleybross. All rights reserved.
// </copyright>
namespace Dapper.MicroCRUD.Tests
{
    using System;
    using Dapper.MicroCRUD.Dialects;
    using Dapper.MicroCRUD.Tests.Utils;

    public abstract class DatabaseFixture
        : IDisposable
    {
        protected DatabaseFixture(string dialectName)
        {
            this.Database = BlankDatabaseFactory.MakeDatabase(dialectName);
            this.DialectName = dialectName;
            this.DatabaseDialect = this.Database.Dialect;
        }

        public string DialectName { get; set; }

        public BlankDatabase Database { get; }

        public IDialect DatabaseDialect { get; set; }

        public void Dispose()
        {
            this.Database.Dispose();
        }
    }
}