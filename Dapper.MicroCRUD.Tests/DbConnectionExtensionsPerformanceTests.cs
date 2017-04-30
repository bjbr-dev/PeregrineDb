// <copyright file="DbConnectionExtensionsPerformanceTests.cs" company="Berkeleybross">
//   Copyright (c) Berkeleybross. All rights reserved.
// </copyright>
namespace Dapper.MicroCRUD.Tests
{
    using System;
    using System.Data;
    using System.Diagnostics;
    using System.Linq;
    using Dapper.MicroCRUD.Dialects;
    using Dapper.MicroCRUD.Tests.Dialects.Postgres;
    using Dapper.MicroCRUD.Tests.Dialects.SqlServer;
    using Dapper.MicroCRUD.Tests.ExampleEntities;
    using FluentAssertions;
    using Xunit;

    public abstract class DbConnectionExtensionsPerformanceTests
    {
        private readonly IDbConnection connection;
        private readonly IDialect dialect;

        protected DbConnectionExtensionsPerformanceTests(DatabaseFixture fixture)
        {
            this.dialect = fixture.DatabaseDialect;
            this.connection = fixture.Database.Connection;
        }

        private long PerformInsert()
        {
            // Arrange
            var entities = Enumerable.Range(0, 30000).Select(i => new SimpleBenchmarkEntity
                {
                    FirstName = $"First Name {i}",
                    LastName = $"Last Name {i}",
                    DateOfBirth = DateTime.Now
                }).ToList();

            var stopWatch = new Stopwatch();
            stopWatch.Start();

            // Act
            using (var transaction = this.connection.BeginTransaction())
            {
                foreach (var entity in entities)
                {
                    this.connection.Insert(entity, transaction, this.dialect);
                }

                transaction.Commit();
            }

            // Assert
            stopWatch.Stop();
            Console.WriteLine($"Performed insert in {stopWatch.ElapsedMilliseconds}ms");

            // Cleanup
            this.connection.DeleteAll<SimpleBenchmarkEntity>(dialect: this.dialect);

            return stopWatch.ElapsedMilliseconds;
        }

        private long PerformInsertRange()
        {
            // Arrange
            var entities = Enumerable.Range(0, 30000).Select(i => new SimpleBenchmarkEntity
                {
                    FirstName = $"First Name {i}",
                    LastName = $"Last Name {i}",
                    DateOfBirth = DateTime.Now
                }).ToList();

            var stopWatch = new Stopwatch();
            stopWatch.Start();

            // Act
            using (var transaction = this.connection.BeginTransaction())
            {
                this.connection.InsertRange(entities, transaction, this.dialect);

                transaction.Commit();
            }

            // Assert
            stopWatch.Stop();
            Console.WriteLine($"Performed insertrange in {stopWatch.ElapsedMilliseconds}ms");

            // Cleanup
            this.connection.DeleteAll<SimpleBenchmarkEntity>(dialect: this.dialect);

            return stopWatch.ElapsedMilliseconds;
        }

        [Collection(nameof(SqlServerCollection))]
        public class SqlServer2012
            : DbConnectionExtensionsPerformanceTests
        {
            public SqlServer2012(SqlServerFixture fixture)
                : base(fixture)
            {
            }

            [Fact]
            public void Takes_less_than_6_seconds_to_insert_30000_rows()
            {
                var timeTaken = this.PerformInsert();
                timeTaken.Should().BeLessThan(6000);
            }

            [Fact]
            public void Takes_less_than_5_seconds_to_InsertRange_30000_rows()
            {
                var timeTaken = this.PerformInsertRange();
                timeTaken.Should().BeLessThan(5000);
            }
        }

        [Collection(nameof(PostgresCollection))]
        public class PostgreSQL
            : DbConnectionExtensionsPerformanceTests
        {
            public PostgreSQL(PostgresFixture fixture)
                : base(fixture)
            {
            }

            [Fact]
            public void Takes_less_than_6_seconds_to_insert_30000_rows()
            {
                var timeTaken = this.PerformInsert();
                timeTaken.Should().BeLessThan(6000);
            }

            [Fact]
            public void Takes_less_than_5_seconds_to_InsertRange_30000_rows()
            {
                var timeTaken = this.PerformInsertRange();
                timeTaken.Should().BeLessThan(5000);
            }
        }
    }
}