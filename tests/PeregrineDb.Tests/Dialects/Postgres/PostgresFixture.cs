// <copyright file="PostgresFixture.cs" company="Berkeleybross">
// Copyright (c) Berkeleybross. All rights reserved.
// </copyright>
namespace Dapper.MicroCRUD.Tests.Dialects.Postgres
{
    public class PostgresFixture
        : DatabaseFixture
    {
        public PostgresFixture()
            : base(nameof(Dialect.PostgreSql))
        {
        }
    }
}