namespace PeregrineDb.Tests.Databases
{
    using System;
    using System.Diagnostics.CodeAnalysis;
    using FluentAssertions;
    using PeregrineDb.Dialects;
    using PeregrineDb.Schema;
    using PeregrineDb.Tests.ExampleEntities;
    using PeregrineDb.Tests.Utils;
    using Xunit;

    public abstract partial class DefaultDatabaseConnectionCrudTests
    {
        public class Get
            : DefaultDatabaseConnectionCrudTests
        {
            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Throws_exception_when_entity_has_no_key(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    database.Insert(new NoKey { Name = "Some Name", Age = 1 });

                    // Act
                    Assert.Throws<InvalidPrimaryKeyException>(() => database.Get<NoKey>("Some Name"));
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Throws_exception_when_entity_is_not_found(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Act
                    Assert.Throws<InvalidOperationException>(() => database.Get<KeyInt32>(5));
                }
            }

            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Finds_entity_by_Int32_primary_key(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var id = database.Insert<int>(new KeyInt32 { Name = "Some Name" });

                    // Act
                    var entity = database.Get<KeyInt32>(id);

                    // Assert
                    entity.Name.Should().Be("Some Name");
                }
            }

            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Finds_entity_by_Int64_primary_key(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var id = database.Insert<long>(new KeyInt64 { Name = "Some Name" });

                    // Act
                    var entity = database.Get<KeyInt64>(id);

                    // Assert
                    entity.Name.Should().Be("Some Name");
                }
            }

            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Finds_entity_by_string_primary_key(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    database.Insert(new KeyString { Name = "Some Name", Age = 42 });

                    // Act
                    var entity = database.Get<KeyString>("Some Name");

                    // Assert
                    entity.Age.Should().Be(42);
                }
            }

            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Finds_entity_by_guid_primary_key(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var id = Guid.NewGuid();
                    database.Insert(new KeyGuid { Id = id, Name = "Some Name" });

                    // Act
                    var entity = database.Get<KeyGuid>(id);

                    // Assert
                    entity.Name.Should().Be("Some Name");
                }
            }

            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Finds_entity_by_composite_key(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    database.Insert(new CompositeKeys { Key1 = 1, Key2 = 1, Name = "Some Name" });
                    var id = new { Key1 = 1, Key2 = 1 };

                    // Act
                    var entity = database.Get<CompositeKeys>(id);

                    // Assert
                    entity.Name.Should().Be("Some Name");
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Finds_entities_in_alternate_schema(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var id = database.Insert<int>(new SchemaOther { Name = "Some Name" });

                    // Act
                    var entity = database.Get<SchemaOther>(id);

                    // Assert
                    entity.Name.Should().Be("Some Name");
                }
            }

            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Ignores_columns_which_are_not_mapped(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var id = database.Insert<int>(new PropertyNotMapped { FirstName = "Bobby", LastName = "DropTables", Age = 10 });

                    // Act
                    var entity = database.Get<PropertyNotMapped>(id);

                    // Assert
                    entity.FirstName.Should().Be("Bobby");
                    entity.LastName.Should().Be("DropTables");
                    entity.FullName.Should().Be("Bobby DropTables");
                    entity.Age.Should().Be(0);
                }
            }
        }

        public class GetFirst
            : DefaultDatabaseConnectionCrudTests
        {
            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Gets_first_matching_result(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    database.Insert(new Dog { Name = "Some Name 1", Age = 10 });
                    database.Insert(new Dog { Name = "Some Name 2", Age = 10 });
                    database.Insert(new Dog { Name = "Some Name 3", Age = 10 });
                    database.Insert(new Dog { Name = "Some Name 4", Age = 11 });

                    // Act
                    var entity = database.GetFirst<Dog>("Name DESC", "WHERE Name LIKE CONCAT(@Name, '%') and Age = @Age", new { Name = "Some Name", Age = 10 });

                    // Assert
                    entity.Should().BeEquivalentTo(new Dog { Name = "Some Name 3", Age = 10 }, o => o.Excluding(e => e.Id));
                }
            }

            [Theory]
            [MemberData(nameof(TestDialects))]
            [SuppressMessage("ReSharper", "AccessToDisposedClosure")]
            public void Throws_exception_when_no_entity_matches(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Act
                    Action act = () => database.GetFirst<Dog>("Name DESC", "WHERE Name LIKE CONCAT(@Name, '%') and Age = @Age", new { Name = "Some Name", Age = 10 });

                    // Assert
                    act.Should().Throw<InvalidOperationException>();
                }
            }
        }

        public class GetFirstWhereObject
            : DefaultDatabaseConnectionCrudTests
        {
            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Gets_first_matching_result(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    database.Insert(new Dog { Name = "Some Name 1", Age = 10 });
                    database.Insert(new Dog { Name = "Some Name 2", Age = 10 });
                    database.Insert(new Dog { Name = "Some Name 3", Age = 10 });
                    database.Insert(new Dog { Name = "Some Name 4", Age = 11 });

                    // Act
                    var entity = database.GetFirst<Dog>("Name DESC", new { Age = 10 });

                    // Assert
                    entity.Should().BeEquivalentTo(new Dog { Name = "Some Name 3", Age = 10 }, o => o.Excluding(e => e.Id));
                }
            }

            [Theory]
            [MemberData(nameof(TestDialects))]
            [SuppressMessage("ReSharper", "AccessToDisposedClosure")]
            public void Throws_exception_when_no_entity_matches(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Act
                    Action act = () => database.GetFirst<Dog>("Name DESC", new { Age = 10 });

                    // Assert
                    act.Should().Throw<InvalidOperationException>();
                }
            }
        }

        public class GetSingle
            : DefaultDatabaseConnectionCrudTests
        {
            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Gets_first_matching_result(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    database.Insert(new Dog { Name = "Some Name 1", Age = 10 });
                    database.Insert(new Dog { Name = "Some Name 2", Age = 11 });

                    // Act
                    var entity = database.GetSingle<Dog>("WHERE Name LIKE CONCAT(@Name, '%') and Age = @Age", new { Name = "Some Name", Age = 10 });

                    // Assert
                    entity.Should().BeEquivalentTo(new Dog { Name = "Some Name 1", Age = 10 }, o => o.Excluding(e => e.Id));
                }
            }

            [Theory]
            [MemberData(nameof(TestDialects))]
            [SuppressMessage("ReSharper", "AccessToDisposedClosure")]
            public void Throws_exception_when_no_entity_matches(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Act
                    Action act = () => database.GetSingle<Dog>("WHERE Name LIKE CONCAT(@Name, '%') and Age = @Age", new { Name = "Some Name", Age = 10 });

                    // Assert
                    act.Should().Throw<InvalidOperationException>();
                }
            }

            [Theory]
            [MemberData(nameof(TestDialects))]
            [SuppressMessage("ReSharper", "AccessToDisposedClosure")]
            public void Throws_exception_when_multiple_entities_match(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    database.Insert(new Dog { Name = "Some Name 1", Age = 10 });
                    database.Insert(new Dog { Name = "Some Name 2", Age = 10 });

                    // Act
                    Action act = () => database.GetSingle<Dog>($"WHERE Age = {10}");

                    // Assert
                    act.Should().Throw<InvalidOperationException>();
                }
            }
        }

        public class GetSingleWhereObject
            : DefaultDatabaseConnectionCrudTests
        {
            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Gets_first_matching_result(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    database.Insert(new Dog { Name = "Some Name 1", Age = 10 });
                    database.Insert(new Dog { Name = "Some Name 2", Age = 11 });

                    // Act
                    var entity = database.GetSingle<Dog>(new { Age = 10 });

                    // Assert
                    entity.Should().BeEquivalentTo(new Dog { Name = "Some Name 1", Age = 10 }, o => o.Excluding(e => e.Id));
                }
            }

            [Theory]
            [MemberData(nameof(TestDialects))]
            [SuppressMessage("ReSharper", "AccessToDisposedClosure")]
            public void Throws_exception_when_no_entity_matches(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Act
                    Action act = () => database.GetSingle<Dog>(new { Age = 10 });

                    // Assert
                    act.Should().Throw<InvalidOperationException>();
                }
            }

            [Theory]
            [MemberData(nameof(TestDialects))]
            [SuppressMessage("ReSharper", "AccessToDisposedClosure")]
            public void Throws_exception_when_multiple_entities_match(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    database.Insert(new Dog { Name = "Some Name 1", Age = 10 });
                    database.Insert(new Dog { Name = "Some Name 2", Age = 10 });

                    // Act
                    Action act = () => database.GetSingle<Dog>(new { Age = 10 });

                    // Assert
                    act.Should().Throw<InvalidOperationException>();
                }
            }
        }
    }
}