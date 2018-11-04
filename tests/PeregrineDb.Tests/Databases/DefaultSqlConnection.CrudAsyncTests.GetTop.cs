namespace PeregrineDb.Tests.Databases
{
    using System;
    using System.Diagnostics.CodeAnalysis;
    using System.Linq;
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
        public class GetTopAsync
            : DefaultDatabaseConnectionCrudTests
        {
            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Returns_less_than_count_if_there_arent_that_many_rows(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    database.Insert(new Dog { Name = "Some Name 1", Age = 10 });
                    var orderBy = dialect is SqlServer2012Dialect ? "[Name] ASC" : "name ASC";

                    // Act
                    var entities = await database.GetTopAsync<Dog>(2, orderBy);

                    // Assert
                    entities.Count().Should().Be(1);
                }
            }

            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Returns_first_N_matching_rows(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    database.Insert(new Dog { Name = "Some Name 4", Age = 11 });
                    database.Insert(new Dog { Name = "Some Name 3", Age = 10 });
                    database.Insert(new Dog { Name = "Some Name 2", Age = 10 });
                    database.Insert(new Dog { Name = "Some Name 1", Age = 10 });

                    var orderBy = dialect is SqlServer2012Dialect ? "[Name] ASC" : "name ASC";

                    // Act
                    var entities = await database.GetTopAsync<Dog>(2, orderBy);

                    // Assert
                    entities.Should().BeEquivalentTo(new[]
                            {
                                new { Name = "Some Name 1", Age = 10 },
                                new { Name = "Some Name 2", Age = 10 }
                            },
                        o => o.WithStrictOrdering());
                }
            }

            /// <summary>
            /// Prevents a SQL injection vector when forgetting to do an order by
            /// </summary>
            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Throws_exception_if_orderby_is_not_a_known_column(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    database.Insert(new Dog { Name = "Some Name 1", Age = 10 });
                    database.Insert(new Dog { Name = "Some Name 2", Age = 10 });
                    database.Insert(new Dog { Name = "Some Name 3", Age = 10 });
                    database.Insert(new Dog { Name = "Some Name 4", Age = 11 });

                    // Act
                    Func<Task> act = async () => await database.GetTopAsync<Dog>(2, "WHERE Age = 10");

                    // Assert
                    act.Should().Throw<Exception>();
                }
            }
        }

        public class GetTopAsyncWhere
            : DefaultDatabaseConnectionCrudTests
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
                    var entities = await database.GetTopAsync<Dog>(2, "name", "WHERE Name LIKE CONCAT(@Name, '%') and Age = @Age", new { Name = "Some Name", Age = 10 });

                    // Assert
                    entities.Should().HaveCount(2);
                }
            }
        }

        public class GetTopAsyncWhereObject
            : DefaultDatabaseConnectionCrudTests
        {
            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Filters_result_by_conditions(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    database.Insert(new Dog { Name = "Some Name 1", Age = 11 });
                    database.Insert(new Dog { Name = "Some Name 2", Age = 10 });
                    database.Insert(new Dog { Name = "Some Name 3", Age = 10 });
                    database.Insert(new Dog { Name = "Some Name 4", Age = 10 });

                    // Act
                    var entities = await database.GetTopAsync<Dog>(2, "name", new { Age = 10 });

                    // Assert
                    entities.Should().BeEquivalentTo(new[]
                            {
                                new { Name = "Some Name 2", Age = 10 },
                                new { Name = "Some Name 3", Age = 10 }
                            },
                        o => o.WithStrictOrdering());
                }
            }
        }
    }
}