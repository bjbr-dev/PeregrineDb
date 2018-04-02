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
                    // Act
                    database.CreateTempTable(Enumerable.Empty<TempNoKey>());

                    // Assert
                    database.Query<dynamic>(new SqlString($@"SELECT * FROM TempNoKey"));

                    // Cleanup
                    database.DropTempTable<TempNoKey>();
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

                    // Act
                    database.CreateTempTable(entities);

                    // Assert
                    database.Count<TempNoKey>().Should().Be(2);

                    // Cleanup
                    database.DropTempTable<TempNoKey>();
                }
            }
        }

        public class DropTempTable
            : DefaultDatabaseConnectionTempTableTests
        {
            [Fact]
            public void Drops_temporary_table()
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.PostgreSql))
                {
                    // Arrange
                    database.CreateTempTable(Enumerable.Empty<TempNoKey>());

                    // Act
                    database.DropTempTable<TempNoKey>();

                    // Assert
                    Action act = () => database.Query<dynamic>($"SELECT * FROM TempNoKey");
                    act.ShouldThrow<Exception>();
                }
            }
        }
    }
}