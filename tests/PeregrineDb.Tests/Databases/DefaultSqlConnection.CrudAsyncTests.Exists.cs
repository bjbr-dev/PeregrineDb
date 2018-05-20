namespace PeregrineDb.Tests.Databases
{
    using System;
    using System.Diagnostics.CodeAnalysis;
    using System.Threading.Tasks;
    using FluentAssertions;
    using PeregrineDb.Dialects;
    using PeregrineDb.Tests.ExampleEntities;
    using PeregrineDb.Tests.Utils;
    using Xunit;

    [SuppressMessage("ReSharper", "StringLiteralAsInterpolationArgument")]
    [SuppressMessage("ReSharper", "AccessToDisposedClosure")]
    public abstract partial class DefaultDatabaseConnectionCrudAsyncTests
    {
        public class ExistsAsyncWhere
            : DefaultDatabaseConnectionCrudTests
        {
            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Returns_false_if_no_entity_matches_conditions(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    database.Insert(new Dog { Name = "Some Name 4", Age = 11 });

                    // Act
                    var result = await database.ExistsAsync<Dog>($"WHERE Age < {11}");

                    // Assert
                    result.Should().BeFalse();
                }
            }

            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Returns_true_if_an_entity_matches_conditions(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    database.Insert(new Dog { Name = "Some Name 1", Age = 10 });

                    // Act
                    var result = await database.ExistsAsync<Dog>($"WHERE Age < {11}");

                    // Assert
                    result.Should().BeTrue();
                }
            }
        }

        public class ExistsAsyncWhereObject
            : DefaultDatabaseConnectionCrudTests
        {
            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Throws_exception_when_conditions_is_null(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Act
                    Func<Task> act = async () => await database.ExistsAsync<Dog>((object)null);

                    // Assert
                    act.ShouldThrow<ArgumentNullException>();
                }
            }

            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Returns_false_if_no_entity_matches_conditions(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    database.Insert(new Dog { Name = "Some Name 4", Age = 11 });

                    // Act
                    var result = await database.ExistsAsync<Dog>(new { Age = 10 });

                    // Assert
                    result.Should().BeFalse();
                }
            }

            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Returns_true_if_an_entity_matches_conditions(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    database.Insert(new Dog { Name = "Some Name 4", Age = 11 });

                    // Act
                    var result = await database.ExistsAsync<Dog>(new { Age = 11 });

                    // Assert
                    result.Should().BeTrue();
                }
            }
        }
    }
}