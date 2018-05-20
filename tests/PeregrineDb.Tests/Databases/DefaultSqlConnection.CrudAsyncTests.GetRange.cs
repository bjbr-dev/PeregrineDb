namespace PeregrineDb.Tests.Databases
{
    using System;
    using System.Diagnostics.CodeAnalysis;
    using System.Linq;
    using System.Threading.Tasks;
    using FluentAssertions;
    using PeregrineDb.Dialects;
    using PeregrineDb.Schema;
    using PeregrineDb.Tests.ExampleEntities;
    using PeregrineDb.Tests.Utils;
    using Xunit;

    [SuppressMessage("ReSharper", "StringLiteralAsInterpolationArgument")]
    [SuppressMessage("ReSharper", "AccessToDisposedClosure")]
    public abstract partial class DefaultDatabaseConnectionCrudAsyncTests
    {
        public class GetRangeAsync
            : DefaultDatabaseConnectionCrudAsyncTests
        {
            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Filters_result_by_conditions(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    database.Insert(new Dog { Name = "Some Name 1", Age = 10 });
                    database.Insert(new Dog { Name = "Some Name 2", Age = 10 });
                    database.Insert(new Dog { Name = "Some Name 3", Age = 10 });
                    database.Insert(new Dog { Name = "Some Name 4", Age = 11 });

                    // Act
                    var dogs = await database.GetRangeAsync<Dog>(
                        $"WHERE Name LIKE CONCAT({"Some Name"}, '%') and Age = {10}");

                    // Assert
                    dogs.Should().HaveCount(3);
                }
            }

            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Returns_everything_when_conditions_is_null(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    database.Insert(new Dog { Name = "Some Name 1", Age = 10 });
                    database.Insert(new Dog { Name = "Some Name 2", Age = 10 });
                    database.Insert(new Dog { Name = "Some Name 3", Age = 10 });
                    database.Insert(new Dog { Name = "Some Name 4", Age = 11 });

                    // Act
                    var entities = await database.GetRangeAsync<Dog>(null);

                    // Assert
                    entities.Count().Should().Be(4);
                }
            }
        }

        public class GetRangeAsyncWhereObject
            : DefaultDatabaseConnectionCrudAsyncTests
        {
            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Throws_exception_when_conditions_is_null(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Act
                    Func<Task> act = async () => await database.GetRangeAsync<Dog>((object)null);

                    // Assert
                    act.ShouldThrow<ArgumentNullException>();
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Returns_all_when_conditions_is_empty(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    database.Insert(new Dog { Name = "Some Name 1", Age = 10 });
                    database.Insert(new Dog { Name = "Some Name 2", Age = 10 });
                    database.Insert(new Dog { Name = "Some Name 3", Age = 10 });
                    database.Insert(new Dog { Name = "Some Name 4", Age = 11 });

                    // Act
                    var entities = await database.GetRangeAsync<Dog>(new { });

                    // Assert
                    entities.Count().Should().Be(4);
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Filters_result_by_conditions(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    database.Insert(new Dog { Name = "Some Name 1", Age = 10 });
                    database.Insert(new Dog { Name = "Some Name 2", Age = 10 });
                    database.Insert(new Dog { Name = "Some Name 3", Age = 10 });
                    database.Insert(new Dog { Name = "Some Name 4", Age = 11 });

                    // Act
                    var entities = await database.GetRangeAsync<Dog>(new { Age = 10 });

                    // Assert
                    entities.Count().Should().Be(3);
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Matches_column_name_case_insensitively(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    database.Insert(new Dog { Name = "Some Name 1", Age = 10 });
                    database.Insert(new Dog { Name = "Some Name 2", Age = 10 });
                    database.Insert(new Dog { Name = "Some Name 3", Age = 10 });
                    database.Insert(new Dog { Name = "Some Name 4", Age = 11 });

                    // Act
                    var entities = await database.GetRangeAsync<Dog>(new { age = 10 });

                    // Assert
                    entities.Count().Should().Be(3);
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            [SuppressMessage("ReSharper", "AccessToDisposedClosure")]
            public void Throws_exception_when_column_not_found(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Act
                    Func<Task> act = async () => await database.GetRangeAsync<Dog>(new { Ages = 10 });

                    // Assert
                    act.ShouldThrow<InvalidConditionSchemaException>();
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task When_value_is_not_null_does_not_find_nulls(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    database.Insert(new PropertyNullable { Name = null });
                    database.Insert(new PropertyNullable { Name = "Some Name 3" });
                    database.Insert(new PropertyNullable { Name = null });

                    // Act
                    var entities = await database.GetRangeAsync<PropertyNullable>(new { Name = "Some Name 3" });

                    // Assert
                    entities.Count().Should().Be(1);
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task When_value_is_null_finds_nulls(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    database.Insert(new PropertyNullable { Name = null });
                    database.Insert(new PropertyNullable { Name = "Some Name 3" });
                    database.Insert(new PropertyNullable { Name = null });

                    // Act
                    var entities = await database.GetRangeAsync<PropertyNullable>(new { Name = (string)null });

                    // Assert
                    entities.Count().Should().Be(2);
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Filters_on_multiple_properties(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    database.Insert(new Dog { Name = "Some Name 1", Age = 10 });
                    database.Insert(new Dog { Name = "Some Name 2", Age = 10 });
                    database.Insert(new Dog { Name = "Some Name 2", Age = 12 });

                    // Act
                    var entities = await database.GetRangeAsync<Dog>(new { Name = "Some Name 2", Age = 10 });

                    // Assert
                    entities.Count().Should().Be(1);
                }
            }
        }
    }
}