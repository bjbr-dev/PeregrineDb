// <copyright file="SqlServerCollection.cs" company="Berkeleybross">
// Copyright (c) Berkeleybross. All rights reserved.
// </copyright>
namespace Dapper.MicroCRUD.Tests.Dialects.SqlServer
{
    using Xunit;

    [CollectionDefinition(nameof(SqlServerCollection))]
    public class SqlServerCollection
        : ICollectionFixture<SqlServerFixture>
    {
    }
}