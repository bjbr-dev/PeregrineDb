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
        public class ExistsWhere
            : DefaultDatabaseConnectionCrudTests
        {
            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Returns_false_if_no_entity_matches_conditions(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    database.Insert(new Dog { Name = "Some Name 4", Age = 11 });

                    // Act
                    var result = database.Exists<Dog>($"WHERE Age < {11}");

                    // Assert
                    result.Should().BeFalse();
                }
            }

            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Returns_true_if_an_entity_matches_conditions(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    database.Insert(new Dog { Name = "Some Name 1", Age = 10 });

                    // Act
                    var result = database.Exists<Dog>($"WHERE Age < {11}");

                    // Assert
                    result.Should().BeTrue();
                }
            }
        }

        public class ExistsWhereObject
            : DefaultDatabaseConnectionCrudTests
        {
            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Throws_exception_when_conditions_is_null(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Act
                    Action act = () => database.Exists<Dog>((object)null);

                    // Assert
                    act.ShouldThrow<ArgumentNullException>();
                }
            }

            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Returns_false_if_no_entity_matches_conditions(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    database.Insert(new Dog { Name = "Some Name 4", Age = 11 });

                    // Act
                    var result = database.Exists<Dog>(new { Age = 10 });

                    // Assert
                    result.Should().BeFalse();
                }
            }

            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Returns_true_if_an_entity_matches_conditions(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    database.Insert(new Dog { Name = "Some Name 4", Age = 11 });

                    // Act
                    var result = database.Exists<Dog>(new { Age = 11 });

                    // Assert
                    result.Should().BeTrue();
                }
            }
        }
    }
}