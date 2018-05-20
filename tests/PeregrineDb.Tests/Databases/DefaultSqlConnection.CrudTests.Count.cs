namespace PeregrineDb.Tests.Databases
{
    using System;
    using FluentAssertions;
    using PeregrineDb.Dialects;
    using PeregrineDb.Tests.ExampleEntities;
    using PeregrineDb.Tests.Utils;
    using Xunit;

    public abstract partial class DefaultDatabaseConnectionCrudTests
    {
        public class Count
            : DefaultDatabaseConnectionCrudTests
        {
            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Counts_all_entities_when_conditions_is_null(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    database.Insert(new Dog { Name = "Some Name 1", Age = 10 });
                    database.Insert(new Dog { Name = "Some Name 2", Age = 10 });
                    database.Insert(new Dog { Name = "Some Name 3", Age = 10 });
                    database.Insert(new Dog { Name = "Some Name 4", Age = 11 });

                    // Act
                    var result = database.Count<Dog>();

                    // Assert
                    result.Should().Be(4);
                }
            }

            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Counts_entities_matching_conditions(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    database.Insert(new Dog { Name = "Some Name 1", Age = 10 });
                    database.Insert(new Dog { Name = "Some Name 2", Age = 10 });
                    database.Insert(new Dog { Name = "Some Name 3", Age = 10 });
                    database.Insert(new Dog { Name = "Some Name 4", Age = 11 });

                    // Act
                    var result = database.Count<Dog>($"WHERE Age < {11}");

                    // Assert
                    result.Should().Be(3);
                }
            }

            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Counts_entities_in_alternate_schema(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    database.Insert<int>(new SchemaOther { Name = "Some Name" });
                    database.Insert<int>(new SchemaOther { Name = "Some Name" });
                    database.Insert<int>(new SchemaOther { Name = "Some Name" });
                    database.Insert<int>(new SchemaOther { Name = "Some Name" });

                    // Act
                    var result = database.Count<SchemaOther>();

                    // Assert
                    result.Should().Be(4);
                }
            }
        }

        public class CountWhereObject
            : DefaultDatabaseConnectionCrudTests
        {

            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Throws_exception_when_conditions_is_null(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Act
                    Action act = () => database.Count<Dog>((object)null);

                    // Assert
                    act.ShouldThrow<ArgumentNullException>();
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Counts_all_entities_when_conditions_is_empty(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    database.Insert(new Dog { Name = "Some Name 1", Age = 10 });
                    database.Insert(new Dog { Name = "Some Name 2", Age = 10 });
                    database.Insert(new Dog { Name = "Some Name 3", Age = 10 });
                    database.Insert(new Dog { Name = "Some Name 4", Age = 11 });

                    // Act
                    var result = database.Count<Dog>(new { });

                    // Assert
                    result.Should().Be(4);
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Counts_entities_matching_conditions(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    database.Insert(new Dog { Name = "Some Name 1", Age = 10 });
                    database.Insert(new Dog { Name = "Some Name 2", Age = 10 });
                    database.Insert(new Dog { Name = "Some Name 3", Age = 10 });
                    database.Insert(new Dog { Name = "Some Name 4", Age = 11 });

                    // Act
                    var result = database.Count<Dog>(new { Age = 10 });

                    // Assert
                    result.Should().Be(3);
                }
            }
        }
    }
}