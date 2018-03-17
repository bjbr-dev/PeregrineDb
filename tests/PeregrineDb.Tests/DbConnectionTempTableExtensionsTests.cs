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
    using Dapper.MicroCRUD.Tests.Dialects.Postgres;
    using Dapper.MicroCRUD.Tests.Dialects.SqlServer;
    using Dapper.MicroCRUD.Tests.ExampleEntities;
    using FluentAssertions;
    using Moq;
    using Xunit;

    public abstract class DbConnectionTempTableExtensionsTests
    {
        private readonly IDbConnection connection;
        private readonly IDialect dialect;

        protected DbConnectionTempTableExtensionsTests(DatabaseFixture fixture)
        {
            this.dialect = fixture.DatabaseDialect;
            this.connection = fixture.Database.Connection;
        }

        private static string GetTableName(DefaultTableNameFactory defaultFactory, Type type, IDialect dialect)
        {
            var tableName = defaultFactory.GetTableName(type, dialect);

            return dialect.Name == nameof(Dialect.SqlServer2012)
                ? "[#" + tableName.Substring(1)
                : tableName;
        }

        public abstract class CreateTempTableAndInsert
            : DbConnectionTempTableExtensionsTests, IDisposable
        {
            private readonly Mock<ITableNameFactory> tableNameFactory;

            protected CreateTempTableAndInsert(DatabaseFixture fixture)
                : base(fixture)
            {
                var defaultFactory = new DefaultTableNameFactory();

                this.tableNameFactory = new Mock<ITableNameFactory>();
                this.tableNameFactory.Setup(f => f.GetTableName(It.IsAny<Type>(), It.IsAny<IDialect>()))
                    .Returns((Type t, IDialect d) => GetTableName(defaultFactory, t, d));

                MicroCRUDConfig.SetTableNameFactory(this.tableNameFactory.Object);
            }

            public void Dispose()
            {
                MicroCRUDConfig.SetTableNameFactory(new DefaultTableNameFactory());
            }

            [Fact]
            public void Creates_a_temp_table()
            {
                // Arrange
                var schema = this.dialect.MakeSchema<TempNoKey>(this.tableNameFactory.Object);

                // Act
                this.connection.CreateTempTable(Enumerable.Empty<TempNoKey>(), dialect: this.dialect);

                // Assert
                this.connection.Query($@"SELECT * FROM {schema.Name}");

                // Cleanup
                this.connection.DropTempTable<TempNoKey>(schema.Name, dialect: this.dialect);
            }

            [Fact]
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
                this.connection.Count<TempNoKey>(dialect: this.dialect).Should().Be(2);

                // Cleanup
                this.connection.DropTempTable<TempNoKey>(schema.Name, dialect: this.dialect);
            }

            [Collection(nameof(PostgresCollection))]
            public class Postgres
                : CreateTempTableAndInsert
            {
                public Postgres(PostgresFixture fixture)
                    : base(fixture)
                {
                }
            }

            [Collection(nameof(SqlServerCollection))]
            public class SqlServer
                : CreateTempTableAndInsert
            {
                public SqlServer(SqlServerFixture fixture)
                    : base(fixture)
                {
                }
            }
        }

        public abstract class DropTempTable
            : DbConnectionTempTableExtensionsTests, IDisposable
        {
            private readonly Mock<ITableNameFactory> tableNameFactory;

            protected DropTempTable(DatabaseFixture fixture)
                : base(fixture)
            {
                var defaultFactory = new DefaultTableNameFactory();

                this.tableNameFactory = new Mock<ITableNameFactory>();
                this.tableNameFactory.Setup(f => f.GetTableName(It.IsAny<Type>(), It.IsAny<IDialect>()))
                    .Returns((Type t, IDialect d) => GetTableName(defaultFactory, t, d));

                MicroCRUDConfig.SetTableNameFactory(this.tableNameFactory.Object);
            }

            public void Dispose()
            {
                MicroCRUDConfig.SetTableNameFactory(new DefaultTableNameFactory());
            }

            [Fact]
            public void Throws_exception_if_names_do_not_match()
            {
                // Arrange
                var schema = this.dialect.MakeSchema<TempNoKey>(this.tableNameFactory.Object);
                this.connection.CreateTempTable(Enumerable.Empty<TempNoKey>(), dialect: this.dialect);

                // Act
                var ex = Assert.Throws<ArgumentException>(() => this.connection.DropTempTable<TempNoKey>("Not a match", dialect: this.dialect));

                // Assert
                ex.Message.Should().Be($"Attempting to drop table '{schema.Name}', but said table name should be 'Not a match'");

                // Cleanup
                this.connection.DropTempTable<TempNoKey>(schema.Name, dialect: this.dialect);
            }

            [Fact]
            public void Drops_temporary_table()
            {
                // Arrange
                var schema = this.dialect.MakeSchema<TempNoKey>(this.tableNameFactory.Object);
                this.connection.CreateTempTable(Enumerable.Empty<TempNoKey>(), dialect: this.dialect);

                // Act
                this.connection.DropTempTable<TempNoKey>(schema.Name, dialect: this.dialect);

                // Assert
                Action act = () => this.connection.Query($"SELECT * FROM {schema.Name}");
                act.ShouldThrow<Exception>();
            }

            [Collection(nameof(PostgresCollection))]
            public class Postgres
                : DropTempTable
            {
                public Postgres(PostgresFixture fixture)
                    : base(fixture)
                {
                }
            }

            [Collection(nameof(SqlServerCollection))]
            public class SqlServer
                : DropTempTable
            {
                public SqlServer(SqlServerFixture fixture)
                    : base(fixture)
                {
                }
            }
        }
    }
}