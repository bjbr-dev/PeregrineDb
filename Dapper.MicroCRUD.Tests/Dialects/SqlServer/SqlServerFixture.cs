// <copyright file="SqlServerFixture.cs" company="Berkeleybross">
// Copyright (c) Berkeleybross. All rights reserved.
// </copyright>
namespace Dapper.MicroCRUD.Tests.Dialects.SqlServer
{
    public class SqlServerFixture
        : DatabaseFixture
    {
        public SqlServerFixture()
            : base(nameof(Dialect.SqlServer2012))
        {
        }
    }
}