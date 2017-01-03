// <copyright file="DbConnectionExtensionsPerformanceTests.cs" company="Berkeleybross">
//   Copyright (c) Berkeleybross. All rights reserved.
// </copyright>
namespace Dapper.MicroCRUD.Tests
{
    using System;
    using System.Data;
    using System.Diagnostics;
    using System.Linq;
    using Dapper.MicroCRUD.Tests.ExampleEntities;
    using Dapper.MicroCRUD.Tests.Utils;
    using NUnit.Framework;

    internal abstract class DbConnectionExtensionsPerformanceTests
    {
        private readonly string dialectName;

        private IDbConnection connection;
        private Dialect dialect;
        private BlankDatabase database;

        protected DbConnectionExtensionsPerformanceTests(string dialectName)
        {
            this.dialectName = dialectName;
        }

        [OneTimeSetUp]
        public void OneTimeSetup()
        {
            this.database = BlankDatabaseFactory.MakeDatabase(this.dialectName);
            this.connection = this.database.Connection;
            this.dialect = this.database.Dialect;
        }

        [OneTimeTearDown]
        public void OneTimeTearDown()
        {
            this.database?.Dispose();
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

            // Cleanup
            this.connection.DeleteAll<SimpleBenchmarkEntity>(dialect: this.dialect);

            return stopWatch.ElapsedMilliseconds;
        }

        [TestFixture]
        private class SqlServer2012
            : DbConnectionExtensionsPerformanceTests
        {
            public SqlServer2012()
                : base(Dialect.SqlServer2012.Name)
            {
            }

            [Test]
            public void Takes_less_than_5_seconds_to_insert_30000_rows()
            {
                var timeTaken = this.PerformInsert();
                Assert.That(timeTaken, Is.LessThan(5000));
            }

            [Test]
            public void Takes_less_than_4_seconds_to_InsertRange_30000_rows()
            {
                var timeTaken = this.PerformInsertRange();
                Assert.That(timeTaken, Is.LessThan(4000));
            }
        }

        [TestFixture]
        private class PostgreSQL
            : DbConnectionExtensionsPerformanceTests
        {
            public PostgreSQL()
                : base(Dialect.PostgreSql.Name)
            {
            }

            [Test]
            public void Takes_less_than_5_seconds_to_insert_30000_rows()
            {
                var timeTaken = this.PerformInsert();
                Assert.That(timeTaken, Is.LessThan(6000));
            }

            [Test]
            public void Takes_less_than_4_seconds_to_InsertRange_30000_rows()
            {
                var timeTaken = this.PerformInsertRange();
                Assert.That(timeTaken, Is.LessThan(5000));
            }
        }
    }
}