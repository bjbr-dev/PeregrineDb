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
        public class CountAsync
            : DefaultDatabaseConnectionCrudAsyncTests
        {
            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Counts_entities(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    database.Insert(new Dog { Name = "Some Name 1", Age = 10 });
                    database.Insert(new Dog { Name = "Some Name 2", Age = 10 });
                    database.Insert(new Dog { Name = "Some Name 3", Age = 10 });
                    database.Insert(new Dog { Name = "Some Name 4", Age = 11 });

                    // Act
                    var result = await database.CountAsync<Dog>();

                    // Assert
                    result.Should().Be(4);
                }
            }

            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Counts_entities_matching_conditions(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    database.Insert(new Dog { Name = "Some Name 1", Age = 10 });
                    database.Insert(new Dog { Name = "Some Name 2", Age = 10 });
                    database.Insert(new Dog { Name = "Some Name 3", Age = 10 });
                    database.Insert(new Dog { Name = "Some Name 4", Age = 11 });

                    // Act
                    var result = await database.CountAsync<Dog>($"WHERE Age < {11}");

                    // Assert
                    result.Should().Be(3);
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Counts_entities_in_alternate_schema(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    database.Insert<int>(new SchemaOther { Name = "Some Name" });
                    database.Insert<int>(new SchemaOther { Name = "Some Name" });
                    database.Insert<int>(new SchemaOther { Name = "Some Name" });
                    database.Insert<int>(new SchemaOther { Name = "Some Name" });

                    // Act
                    var result = await database.CountAsync<SchemaOther>();

                    // Assert
                    result.Should().Be(4);
                }
            }
        }

        public class CountAsyncWhereObject
            : DefaultDatabaseConnectionCrudAsyncTests
        {
            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Throws_exception_when_conditions_is_null(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Act
                    Func<Task> act = async () => await database.CountAsync<Dog>((object)null);

                    // Assert
                    act.Should().Throw<ArgumentNullException>();
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Counts_all_entities_when_conditions_is_empty(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    database.Insert(new Dog { Name = "Some Name 1", Age = 10 });
                    database.Insert(new Dog { Name = "Some Name 2", Age = 10 });
                    database.Insert(new Dog { Name = "Some Name 3", Age = 10 });
                    database.Insert(new Dog { Name = "Some Name 4", Age = 11 });

                    // Act
                    var result = await database.CountAsync<Dog>(new { });

                    // Assert
                    result.Should().Be(4);
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Counts_entities_matching_conditions(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    database.Insert(new Dog { Name = "Some Name 1", Age = 10 });
                    database.Insert(new Dog { Name = "Some Name 2", Age = 10 });
                    database.Insert(new Dog { Name = "Some Name 3", Age = 10 });
                    database.Insert(new Dog { Name = "Some Name 4", Age = 11 });

                    // Act
                    var result = await database.CountAsync<Dog>(new { Age = 10 });

                    // Assert
                    result.Should().Be(3);
                }
            }
        }
    }
}