// <copyright file="DbConnectionTempTableExtensionsTests.cs" company="Berkeleybross">
// Copyright (c) Berkeleybross. All rights reserved.
// </copyright>
namespace Dapper.MicroCRUD.Tests
{
    using System;
    using System.Data;
    using System.Linq;
    using Dapper.MicroCRUD.Dialects;
    using Dapper.MicroCRUD.Schema;
    using Dapper.MicroCRUD.Tests.ExampleEntities;
    using Dapper.MicroCRUD.Tests.Utils;
    using Moq;
    using NCrunch.Framework;
    using NUnit.Framework;

    [ExclusivelyUses("Database")]
    [Parallelizable(ParallelScope.None)]
    [TestFixtureSource(typeof(BlankDatabaseFactory), nameof(BlankDatabaseFactory.PossibleDialects))]
    public class DbConnectionTempTableExtensionsTests
    {
        private readonly string dialectName;

        private IDbConnection connection;
        private IDialect dialect;
        private BlankDatabase database;

        public DbConnectionTempTableExtensionsTests(string dialectName)
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

        private static string GetTableName(DefaultTableNameFactory defaultFactory, Type type, IDialect dialect)
        {
            var tableName = defaultFactory.GetTableName(type, dialect);

            return dialect.Name == nameof(Dialect.SqlServer2012)
                ? "[#" + tableName.Substring(1)
                : tableName;
        }

        private class CreateTempTableAndInsert
            : DbConnectionTempTableExtensionsTests
        {
            private Mock<ITableNameFactory> tableNameFactory;

            public CreateTempTableAndInsert(string dialectName)
                : base(dialectName)
            {
            }

            [SetUp]
            public void SetUp()
            {
                var defaultFactory = new DefaultTableNameFactory();

                this.tableNameFactory = new Mock<ITableNameFactory>();
                this.tableNameFactory.Setup(f => f.GetTableName(It.IsAny<Type>(), It.IsAny<IDialect>()))
                                .Returns((Type t, IDialect d) => GetTableName(defaultFactory, t, d));

                MicroCRUDConfig.SetTableNameFactory(this.tableNameFactory.Object);
            }

            [TearDown]
            public void TearDown()
            {
                MicroCRUDConfig.SetTableNameFactory(new DefaultTableNameFactory());
            }

            [Test]
            public void Creates_a_temp_table()
            {
                // Arrange
                var entities = new[]
                    {
                        new TempNoKey { Name = "Bobby", Age = 4 },
                        new TempNoKey { Name = "Jimmy", Age = 10 }
                    };
                var schema = this.dialect.MakeSchema<TempNoKey>(this.tableNameFactory.Object);

                // Act
                this.connection.CreateTempTable(Enumerable.Empty<TempNoKey>(), dialect: this.dialect);

                // Assert
                this.connection.Query($@"SELECT * FROM {schema.Name}");

                // Cleanup
                this.connection.DropTempTable<TempNoKey>(schema.Name, dialect: this.dialect);
            }

            [Test]
            public void Adds_entities_to_temp_table()
            {
                // Arrange
                var entities = new[]
                    {
                        new TempNoKey { Name = "Bobby", Age = 4 },
                        new TempNoKey { Name = "Jimmy", Age = 10 }
                    };
                var schema = this.dialect.MakeSchema<TempNoKey>(this.tableNameFactory.Object);

                // Act
                this.connection.CreateTempTable(entities, dialect: this.dialect);

                // Assert
                Assert.AreEqual(2, this.connection.Count<TempNoKey>(dialect: this.dialect));

                // Cleanup
                this.connection.DropTempTable<TempNoKey>(schema.Name, dialect: this.dialect);
            }
        }

        private class DropTempTable
            : DbConnectionTempTableExtensionsTests
        {
            private Mock<ITableNameFactory> tableNameFactory;

            public DropTempTable(string dialectName)
                : base(dialectName)
            {
            }

            [SetUp]
            public void SetUp()
            {
                var defaultFactory = new DefaultTableNameFactory();

                this.tableNameFactory = new Mock<ITableNameFactory>();
                this.tableNameFactory.Setup(f => f.GetTableName(It.IsAny<Type>(), It.IsAny<IDialect>()))
                                .Returns((Type t, IDialect d) => GetTableName(defaultFactory, t, d));

                MicroCRUDConfig.SetTableNameFactory(this.tableNameFactory.Object);
            }

            [TearDown]
            public void TearDown()
            {
                MicroCRUDConfig.SetTableNameFactory(new DefaultTableNameFactory());
            }

            [Test]
            public void Throws_exception_if_names_do_not_match()
            {
                // Arrange
                var schema = this.dialect.MakeSchema<TempNoKey>(this.tableNameFactory.Object);
                this.connection.CreateTempTable(Enumerable.Empty<TempNoKey>(), dialect: this.dialect);

                // Act
                var ex = Assert.Throws<ArgumentException>(() => this.connection.DropTempTable<TempNoKey>("Not a match", dialect: this.dialect));

                // Assert
                Assert.AreEqual($"Attempting to drop table '{schema.Name}', but said table name should be 'Not a match'", ex.Message);

                // Cleanup
                this.connection.DropTempTable<TempNoKey>(schema.Name, dialect: this.dialect);
            }

            [Test]
            public void Drops_temporary_table()
            {
                // Arrange
                var schema = this.dialect.MakeSchema<TempNoKey>(this.tableNameFactory.Object);
                this.connection.CreateTempTable(Enumerable.Empty<TempNoKey>(), dialect: this.dialect);

                // Act
                this.connection.DropTempTable<TempNoKey>(schema.Name, dialect: this.dialect);

                // Assert
                Assert.Catch(() => this.connection.Query($"SELECT * FROM {schema.Name}"));
            }
        }
    }
}