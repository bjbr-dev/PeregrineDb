namespace PeregrineDb.Tests.Databases
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using FluentAssertions;
    using Moq;
    using PeregrineDb;
    using PeregrineDb.Dialects;
    using PeregrineDb.Schema;
    using PeregrineDb.Tests.ExampleEntities;
    using PeregrineDb.Tests.Utils;
    using Xunit;

    public abstract class DefaultDatabaseConnectionTempTableTests
    {
        public static IEnumerable<object[]> TestDialects() => new[]
            {
                new[] { Dialect.SqlServer2012 },
                new[] { Dialect.PostgreSql }
            };

        private static string GetTableName(AtttributeTableNameFactory atttributeFactory, Type type, IDialect dialect)
        {
            var tableName = atttributeFactory.GetTableName(type, dialect);

            return dialect.Name == nameof(Dialect.SqlServer2012)
                ? "[#" + tableName.Substring(1)
                : tableName;
        }

        public class CreateTempTableAndInsert
            : DefaultDatabaseConnectionTempTableTests
        {
            private readonly Mock<ITableNameFactory> tableNameFactory;

            public CreateTempTableAndInsert()
            {
                var defaultFactory = new AtttributeTableNameFactory();

                this.tableNameFactory = new Mock<ITableNameFactory>();
                this.tableNameFactory.Setup(f => f.GetTableName(It.IsAny<Type>(), It.IsAny<IDialect>()))
                    .Returns((Type t, IDialect d) => GetTableName(defaultFactory, t, d));
            }

            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Creates_a_temp_table(IDialect dialect)
            {
                using (var instance = BlankDatabaseFactory.MakeDatabase(DefaultPeregrineConfig.SqlServer2012.WithTableNameFactory(this.tableNameFactory.Object)))
                {
                    // Arrange
                    var database = instance.Item;
                    var schema = database.Config.MakeSchema<TempNoKey>();

                    // Act
                    database.CreateTempTable(Enumerable.Empty<TempNoKey>());

                    // Assert
                    database.Query<dynamic>($@"SELECT * FROM {schema.Name}");

                    // Cleanup
                    database.DropTempTable<TempNoKey>(schema.Name);
                }
            }

            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Adds_entities_to_temp_table(IDialect dialect)
            {
                using (var instance = BlankDatabaseFactory.MakeDatabase(DefaultPeregrineConfig.SqlServer2012.WithTableNameFactory(this.tableNameFactory.Object)))
                {
                    // Arrange
                    var database = instance.Item;
                    var entities = new[]
                        {
                            new TempNoKey { Name = "Bobby", Age = 4 },
                            new TempNoKey { Name = "Jimmy", Age = 10 }
                        };
                    var schema = database.Config.MakeSchema<TempNoKey>();

                    // Act
                    database.CreateTempTable(entities);

                    // Assert
                    database.Count<TempNoKey>().Should().Be(2);

                    // Cleanup
                    database.DropTempTable<TempNoKey>(schema.Name);
                }
            }
        }

        public class DropTempTable
            : DefaultDatabaseConnectionTempTableTests
        {
            private readonly Mock<ITableNameFactory> tableNameFactory;

            public DropTempTable()
            {
                var defaultFactory = new AtttributeTableNameFactory();

                this.tableNameFactory = new Mock<ITableNameFactory>();
                this.tableNameFactory.Setup(f => f.GetTableName(It.IsAny<Type>(), It.IsAny<IDialect>()))
                    .Returns((Type t, IDialect d) => GetTableName(defaultFactory, t, d));
            }

            [Theory]
            [MemberData(nameof(TestDialects), MemberType = typeof(DefaultDatabaseConnectionTempTableTests))]
            public void Throws_exception_if_names_do_not_match(IDialect dialect)
            {
                using (var instance = BlankDatabaseFactory.MakeDatabase(DefaultPeregrineConfig.SqlServer2012.WithTableNameFactory(this.tableNameFactory.Object)))
                {
                    // Arrange
                    var database = instance.Item;
                    var schema = database.Config.MakeSchema<TempNoKey>();
                    database.CreateTempTable(Enumerable.Empty<TempNoKey>());

                    // Act
                    var ex = Assert.Throws<ArgumentException>(() => database.DropTempTable<TempNoKey>("Not a match"));

                    // Assert
                    ex.Message.Should().Be($"Attempting to drop table '{schema.Name}', but said table name should be 'Not a match'");

                    // Cleanup
                    database.DropTempTable<TempNoKey>(schema.Name);
                }
            }

            [Theory]
            [MemberData(nameof(TestDialects), MemberType = typeof(DefaultDatabaseConnectionTempTableTests))]
            public void Drops_temporary_table(IDialect dialect)
            {
                using (var instance = BlankDatabaseFactory.MakeDatabase(DefaultPeregrineConfig.SqlServer2012.WithTableNameFactory(this.tableNameFactory.Object)))
                {
                    // Arrange
                    var database = instance.Item;
                    var schema = database.Config.MakeSchema<TempNoKey>();
                    database.CreateTempTable(Enumerable.Empty<TempNoKey>());

                    // Act
                    database.DropTempTable<TempNoKey>(schema.Name);

                    // Assert
                    Action act = () => database.Query<dynamic>($"SELECT * FROM {schema.Name}");
                    act.ShouldThrow<Exception>();
                }
            }
        }
    }
}