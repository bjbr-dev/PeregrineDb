namespace PeregrineDb.Tests.Databases
{
    using System;
    using System.Diagnostics.CodeAnalysis;
    using FluentAssertions;
    using PeregrineDb.Dialects;
    using PeregrineDb.Tests.ExampleEntities;
    using PeregrineDb.Tests.Utils;
    using Xunit;

    public abstract partial class DefaultDatabaseConnectionCrudTests
    {
        public class DeleteId
            : DefaultDatabaseConnectionCrudTests
        {

            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Deletes_the_entity_with_the_specified_id(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange

                    var id = database.Insert<int>(new Dog { Name = "Some name", Age = 10 });

                    // Act
                    database.Delete<Dog>(id);

                    // Assert
                    database.Find<Dog>(id).Should().BeNull();
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Deletes_entity_with_string_key(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    database.Insert(new KeyString { Name = "Some Name", Age = 10 });

                    // Act
                    database.Delete<KeyString>("Some Name");
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Deletes_entity_with_guid_key(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var id = Guid.NewGuid();
                    database.Insert(new KeyGuid { Id = id, Name = "Some Name" });

                    // Act
                    database.Delete<KeyGuid>(id);
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Deletes_entity_with_composite_keys(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var id = new { Key1 = 5, Key2 = 20 };
                    var entity = new CompositeKeys { Key1 = 5, Key2 = 20, Name = "Some Name" };
                    database.Insert(entity);

                    // Act
                    database.Delete<CompositeKeys>(id);
                }
            }
        }

        public class DeleteEntity
            : DefaultDatabaseConnectionCrudTests
        {
            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Deletes_entity_with_matching_key(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var id = database.Insert<int>(new Dog { Name = "Some name", Age = 10 });

                    // Act
                    var entity = database.Find<Dog>(id);
                    database.Delete(entity);

                    // Assert
                    database.Find<Dog>(id).Should().BeNull();
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Deletes_entity_with_composite_keys(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var id = new { Key1 = 5, Key2 = 20 };
                    var entity = new CompositeKeys { Key1 = 5, Key2 = 20, Name = "Some Name" };
                    database.Insert(entity);

                    // Act
                    database.Delete(entity);

                    // Assert
                    database.Find<CompositeKeys>(id).Should().BeNull();
                }
            }
        }

        public class DeleteRange
            : DefaultDatabaseConnectionCrudTests
        {
            [Theory]
            [MemberData(nameof(TestDialectsWithData), new object[] { null })]
            [MemberData(nameof(TestDialectsWithData), "")]
            [MemberData(nameof(TestDialectsWithData), " ")]
            [MemberData(nameof(TestDialectsWithData), "HAVING Age = 10")]
            [MemberData(nameof(TestDialectsWithData), "WHERE")]
            public void Throws_exception_if_conditions_does_not_contain_where_clause(IDialect dialect, string conditions)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Act / Assert
                    Assert.Throws<ArgumentException>(() => database.DeleteRange<Dog>(conditions));
                }
            }

            [Theory]
            [MemberData(nameof(TestDialectsWithData), "Where Age = 10")]
            [MemberData(nameof(TestDialectsWithData), "where Age = 10")]
            [MemberData(nameof(TestDialectsWithData), "WHERE Age = 10")]
            [SuppressMessage("ReSharper", "AccessToDisposedClosure")]
            public void Allows_any_capitalization_of_where_clause(IDialect dialect, string conditions)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Act
                    Action act = () => database.DeleteRange<Dog>(conditions);

                    // Assert
                    act.Should().NotThrow();
                }
            }

            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Deletes_all_matching_entities(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    database.Insert(new Dog { Name = "Some Name 1", Age = 10 });
                    database.Insert(new Dog { Name = "Some Name 2", Age = 10 });
                    database.Insert(new Dog { Name = "Some Name 3", Age = 10 });
                    database.Insert(new Dog { Name = "Some Name 4", Age = 11 });

                    // Act
                    var result = database.DeleteRange<Dog>($"WHERE Age = {10}");

                    // Assert
                    result.NumRowsAffected.Should().Be(3);
                    database.Count<Dog>().Should().Be(1);
                }
            }
        }

        public class DeleteRangeWhereObject
            : DefaultDatabaseConnectionCrudTests
        {
            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Throws_exception_if_conditions_is_null(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Act / Assert
                    Assert.Throws<ArgumentNullException>(() => database.DeleteRange<Dog>((object)null));
                }
            }

            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Throws_exception_if_conditions_is_empty(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Act / Assert
                    Assert.Throws<ArgumentException>(() => database.DeleteRange<Dog>(new { }));
                }
            }

            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Deletes_all_matching_entities(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    database.Insert(new Dog { Name = "Some Name 1", Age = 10 });
                    database.Insert(new Dog { Name = "Some Name 2", Age = 10 });
                    database.Insert(new Dog { Name = "Some Name 3", Age = 10 });
                    database.Insert(new Dog { Name = "Some Name 4", Age = 11 });

                    // Act
                    var result = database.DeleteRange<Dog>(new { Age = 10 });

                    // Assert
                    result.NumRowsAffected.Should().Be(3);
                    database.Count<Dog>().Should().Be(1);
                }
            }
        }

        public class DeleteAll
            : DefaultDatabaseConnectionCrudTests
        {
            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Deletes_all_entities(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    database.Insert(new Dog { Name = "Some Name 1", Age = 10 });
                    database.Insert(new Dog { Name = "Some Name 2", Age = 10 });
                    database.Insert(new Dog { Name = "Some Name 3", Age = 10 });
                    database.Insert(new Dog { Name = "Some Name 4", Age = 11 });

                    // Act
                    var result = database.DeleteAll<Dog>();

                    // Assert
                    result.NumRowsAffected.Should().Be(4);
                    database.Count<Dog>().Should().Be(0);
                }
            }
        }
    }
}