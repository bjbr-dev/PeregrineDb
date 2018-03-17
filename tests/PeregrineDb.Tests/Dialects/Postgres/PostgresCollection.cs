// <copyright file="PostgresCollection.cs" company="Berkeleybross">
// Copyright (c) Berkeleybross. All rights reserved.
// </copyright>
namespace Dapper.MicroCRUD.Tests.Dialects.Postgres
{
    using Xunit;

    [CollectionDefinition(nameof(PostgresCollection))]
    public class PostgresCollection
        : ICollectionFixture<PostgresFixture>
    {
    }
}