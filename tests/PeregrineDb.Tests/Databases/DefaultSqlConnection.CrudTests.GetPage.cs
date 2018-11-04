namespace PeregrineDb.Tests.Databases
{
    using System.Linq;
    using FluentAssertions;
    using Pagination;
    using PeregrineDb.Dialects;
    using PeregrineDb.Tests.ExampleEntities;
    using PeregrineDb.Tests.Utils;
    using Xunit;

    public abstract partial class DefaultDatabaseConnectionCrudTests
    {
        public class GetPage
            : DefaultDatabaseConnectionCrudTests
        {
            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Returns_empty_list_when_there_are_no_entities(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Act
                    var pageBuilder = new PageIndexPageBuilder(1, 10);
                    var entities = database.GetPage<Dog>(pageBuilder, null, "Age");

                    // Assert
                    entities.Items.Count().Should().Be(0);
                }
            }

            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Filters_result_by_conditions(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    database.Insert(new Dog { Name = "Some Name 1", Age = 10 });
                    database.Insert(new Dog { Name = "Some Name 2", Age = 10 });
                    database.Insert(new Dog { Name = "Some Name 3", Age = 10 });
                    database.Insert(new Dog { Name = "Some Name 4", Age = 11 });

                    // Act
                    var entities = database.GetPage<Dog>(
                        new PageIndexPageBuilder(1, 10),
                        "Age",
                        "WHERE Name LIKE CONCAT(@Name, '%') and Age = @Age",
                        new { Name = "Some Name", Age = 10 }).Items;

                    // Assert
                    entities.Count().Should().Be(3);
                }
            }

            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Gets_first_page(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    database.Insert(new Dog { Name = "Some Name 1", Age = 10 });
                    database.Insert(new Dog { Name = "Some Name 2", Age = 10 });
                    database.Insert(new Dog { Name = "Some Name 3", Age = 10 });
                    database.Insert(new Dog { Name = "Some Name 4", Age = 11 });

                    // Act
                    var entities = database.GetPage<Dog>(
                        new PageIndexPageBuilder(1, 2),
                        "Age DESC",
                        "WHERE Name LIKE CONCAT(@Name, '%') and Age = @Age",
                        new { Name = "Some Name", Age = 10 }).Items;

                    // Assert
                    entities.Count().Should().Be(2);
                    entities[0].Name.Should().Be("Some Name 1");
                    entities[1].Name.Should().Be("Some Name 2");
                }
            }

            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Gets_second_page(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    database.Insert(new Dog { Name = "Some Name 1", Age = 10 });
                    database.Insert(new Dog { Name = "Some Name 2", Age = 10 });
                    database.Insert(new Dog { Name = "Some Name 3", Age = 10 });
                    database.Insert(new Dog { Name = "Some Name 4", Age = 11 });

                    // Act
                    var entities = database.GetPage<Dog>(
                        new PageIndexPageBuilder(2, 2),
                        "Age DESC",
                        "WHERE Name LIKE CONCAT(@Name, '%') and Age = @Age",
                        new { Name = "Some Name", Age = 10 }).Items;

                    // Assert
                    entities.Count().Should().Be(1);
                    entities[0].Name.Should().Be("Some Name 3");
                }
            }

            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Returns_empty_set_past_last_page(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    database.Insert(new Dog { Name = "Some Name 1", Age = 10 });
                    database.Insert(new Dog { Name = "Some Name 2", Age = 10 });
                    database.Insert(new Dog { Name = "Some Name 3", Age = 10 });
                    database.Insert(new Dog { Name = "Some Name 4", Age = 11 });

                    // Act
                    var entities = database.GetPage<Dog>(
                        new PageIndexPageBuilder(3, 2),
                        "Age DESC",
                        "WHERE Name LIKE CONCAT(@Name, '%') and Age = @Age",
                        new { Name = "Some Name", Age = 10 }).Items;

                    // Assert
                    entities.Should().BeEmpty();
                }
            }

            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Returns_page_from_everything_when_conditions_is_null(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    database.Insert(new Dog { Name = "Some Name 1", Age = 10 });
                    database.Insert(new Dog { Name = "Some Name 2", Age = 10 });
                    database.Insert(new Dog { Name = "Some Name 3", Age = 10 });
                    database.Insert(new Dog { Name = "Some Name 4", Age = 11 });

                    // Act
                    var entities = database.GetPage<Dog>(new PageIndexPageBuilder(2, 2), "Age DESC").Items;

                    // Assert
                    entities.Count().Should().Be(2);
                }
            }
        }

        public class GetPageWhereObject
            : DefaultDatabaseConnectionCrudTests
        {
            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Returns_empty_list_when_there_are_no_entities(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange

                    // Act
                    var pageBuilder = new PageIndexPageBuilder(1, 10);
                    var entities = database.GetPage<Dog>(pageBuilder, "Age", new { Age = 10 });

                    // Assert
                    entities.Items.Should().BeEmpty();
                }
            }

            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Filters_result_by_conditions(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    database.Insert(new Dog { Name = "Some Name 1", Age = 10 });
                    database.Insert(new Dog { Name = "Some Name 2", Age = 10 });
                    database.Insert(new Dog { Name = "Some Name 3", Age = 10 });
                    database.Insert(new Dog { Name = "Some Name 4", Age = 11 });

                    // Act
                    var pageBuilder = new PageIndexPageBuilder(1, 10);
                    var entities = database.GetPage<Dog>(pageBuilder, "Age", new { Age = 10 });

                    // Assert
                    entities.Items.Count().Should().Be(3);
                }
            }

            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Gets_first_page(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    database.Insert(new Dog { Name = "Some Name 1", Age = 10 });
                    database.Insert(new Dog { Name = "Some Name 2", Age = 10 });
                    database.Insert(new Dog { Name = "Some Name 3", Age = 10 });
                    database.Insert(new Dog { Name = "Some Name 4", Age = 11 });

                    // Act
                    var pageBuilder = new PageIndexPageBuilder(1, 2);
                    var page = database.GetPage<Dog>(pageBuilder, "Age", new { Age = 10 });
                    var entities = page.Items;

                    // Assert
                    entities.Count().Should().Be(2);
                    entities[0].Name.Should().Be("Some Name 1");
                    entities[1].Name.Should().Be("Some Name 2");
                }
            }

            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Gets_second_page(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    database.Insert(new Dog { Name = "Some Name 1", Age = 10 });
                    database.Insert(new Dog { Name = "Some Name 2", Age = 10 });
                    database.Insert(new Dog { Name = "Some Name 3", Age = 10 });
                    database.Insert(new Dog { Name = "Some Name 4", Age = 11 });

                    // Act
                    var pageBuilder = new PageIndexPageBuilder(2, 2);
                    var page = database.GetPage<Dog>(pageBuilder, "Age", new { Age = 10 });
                    var entities = page.Items;

                    // Assert
                    entities.Count().Should().Be(1);
                    entities[0].Name.Should().Be("Some Name 3");
                }
            }

            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Returns_empty_set_past_last_page(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    database.Insert(new Dog { Name = "Some Name 1", Age = 10 });
                    database.Insert(new Dog { Name = "Some Name 2", Age = 10 });
                    database.Insert(new Dog { Name = "Some Name 3", Age = 10 });
                    database.Insert(new Dog { Name = "Some Name 4", Age = 11 });

                    // Act
                    var pageBuilder = new PageIndexPageBuilder(3, 2);
                    var page = database.GetPage<Dog>(pageBuilder, "Age", new { Age = 10 });
                    var entities = page.Items;

                    // Assert
                    entities.Should().BeEmpty();
                }
            }
        }
    }
}