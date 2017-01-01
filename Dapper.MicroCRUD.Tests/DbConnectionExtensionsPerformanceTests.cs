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
    using NCrunch.Framework;
    using NUnit.Framework;

    [ExclusivelyUses("Database")]
    [Parallelizable(ParallelScope.None)]
    [TestFixtureSource(typeof(BlankDatabaseFactory), nameof(BlankDatabaseFactory.PossibleDialects))]
    public class DbConnectionExtensionsPerformanceTests
    {
        private readonly string dialectName;

        private IDbConnection connection;
        private Dialect dialect;
        private BlankDatabase database;

        public DbConnectionExtensionsPerformanceTests(string dialectName)
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

        private class InsertAndReturnKey
            : DbConnectionExtensionsPerformanceTests
        {
            public InsertAndReturnKey(string dialectName)
                : base(dialectName)
            {
            }

            [Test]
            public void Takes_less_than_5_seconds_to_insert_30000_rows()
            {
                // Arrange
                var entities = Enumerable.Range(0, 30000).Select(i => new SimpleBenchmarkEntity
                    {
                        FirstName = $"First Name {i}",
                        LastName = $"Last Name {i}",
                        DateOfBirth = DateTime.Now
                    });

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
                Assert.That(stopWatch.ElapsedMilliseconds, Is.LessThan(5000));

                // Cleanup
                this.connection.DeleteAll<SimpleBenchmarkEntity>(dialect: this.dialect);
            }
        }
    }
}