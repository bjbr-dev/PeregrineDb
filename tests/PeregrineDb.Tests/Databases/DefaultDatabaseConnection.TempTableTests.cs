namespace PeregrineDb.Tests.Databases
{
    using System;
    using System.Linq;
    using FluentAssertions;
    using PeregrineDb;
    using PeregrineDb.Tests.ExampleEntities;
    using PeregrineDb.Tests.Utils;
    using Xunit;

    public abstract class DefaultDatabaseConnectionTempTableTests
    {
        public class CreateTempTableAndInsert
            : DefaultDatabaseConnectionTempTableTests
        {
            [Fact]
            public void Creates_a_temp_table()
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.PostgreSql))
                {
                    // Arrange
                    var schema = database.Config.MakeSchema<TempNoKey>();

                    // Act
                    database.CreateTempTable(Enumerable.Empty<TempNoKey>());

                    // Assert
                    database.Query<dynamic>(new SqlString($@"SELECT * FROM {schema.Name}"));

                    // Cleanup
                    database.DropTempTable<TempNoKey>(schema.Name);
                }
            }

            [Fact]
            public void Adds_entities_to_temp_table()
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.PostgreSql))
                {
                    // Arrange
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
            [Fact]
            public void Throws_exception_if_names_do_not_match()
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.PostgreSql))
                {
                    // Arrange
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

            [Fact]
            public void Drops_temporary_table()
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.PostgreSql))
                {
                    // Arrange
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