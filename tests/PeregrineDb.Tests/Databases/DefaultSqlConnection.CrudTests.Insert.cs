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
        public class Insert
            : DefaultDatabaseConnectionCrudTests
        {
            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Inserts_entity_with_int32_key(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var entity = new KeyInt32 { Name = "Some Name" };

                    // Act
                    database.Insert(entity);

                    // Assert
                    database.Count<KeyInt32>().Should().Be(1);
                }
            }

            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Inserts_entity_with_int64_key(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var entity = new KeyInt64 { Name = "Some Name" };

                    // Act
                    database.Insert(entity);

                    // Assert
                    database.Count<KeyInt64>().Should().Be(1);
                }
            }

            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Inserts_entities_with_composite_keys(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var entity = new CompositeKeys { Key1 = 2, Key2 = 3, Name = "Some Name" };

                    // Act
                    database.Insert(entity);

                    // Assert
                    database.Count<CompositeKeys>().Should().Be(1);
                }
            }

            [Theory]
            [MemberData(nameof(TestDialects))]
            [SuppressMessage("ReSharper", "AccessToDisposedClosure")]
            public void Does_not_allow_part_of_composite_key_to_be_null(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var entity = new CompositeKeys { Key1 = null, Key2 = 5, Name = "Some Name" };

                    // Act
                    Action act = () => database.Insert(entity);

                    // Assert
                    act.ShouldThrow<Exception>();
                }
            }

            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Inserts_entities_with_string_key(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var entity = new KeyString { Name = "Some Name", Age = 10 };

                    // Act
                    database.Insert(entity);

                    // Assert
                    database.Count<KeyString>().Should().Be(1);
                }
            }

            [Theory]
            [MemberData(nameof(TestDialects))]
            [SuppressMessage("ReSharper", "AccessToDisposedClosure")]
            public void Does_not_allow_string_key_to_be_null(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var entity = new KeyString { Name = null, Age = 10 };

                    // Act
                    Action act = () => database.Insert(entity);

                    // Assert
                    act.ShouldThrow<Exception>();
                }
            }

            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Inserts_entities_with_guid_key(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var entity = new KeyGuid { Id = Guid.NewGuid(), Name = "Some Name" };

                    // Act
                    database.Insert(entity);

                    // Assert
                    database.Count<KeyGuid>().Should().Be(1);
                }
            }

            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Uses_key_attribute_to_determine_key(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var entity = new KeyAlias { Name = "Some Name" };

                    // Act
                    database.Insert(entity);

                    // Assert
                    database.Count<KeyAlias>().Should().Be(1);
                }
            }

            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Inserts_into_other_schemas(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var entity = new SchemaOther { Name = "Some name" };

                    // Act
                    database.Insert(entity);

                    // Assert
                    database.Count<SchemaOther>().Should().Be(1);
                }
            }

            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Ignores_columns_which_are_not_mapped(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var entity = new PropertyNotMapped { FirstName = "Bobby", LastName = "DropTables", Age = 10 };

                    // Act
                    database.Insert(entity);

                    // Assert
                    database.Count<PropertyNotMapped>().Should().Be(1);
                }
            }
        }

        public class InsertAndReturnKey
            : DefaultDatabaseConnectionCrudTests
        {
            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Throws_exception_when_entity_has_no_key(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Act
                    Assert.Throws<InvalidPrimaryKeyException>(() => database.Insert<int>(new NoKey()));
                }
            }

            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Throws_exception_when_entity_has_composite_keys(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Act
                    Assert.Throws<InvalidPrimaryKeyException>(() => database.Insert<int>(new CompositeKeys()));
                }
            }

            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Throws_exception_for_string_keys(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var entity = new KeyString { Name = "Some Name", Age = 10 };

                    // Act / Assert
                    Assert.Throws<InvalidPrimaryKeyException>(() => database.Insert<string>(entity));
                }
            }

            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Throws_exception_for_guid_keys(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var entity = new KeyGuid { Id = Guid.NewGuid(), Name = "Some Name" };

                    // Act / Assert
                    Assert.Throws<InvalidPrimaryKeyException>(() => database.Insert<Guid>(entity));
                }
            }

            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Inserts_entity_with_int32_primary_key(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Act
                    var id = database.Insert<int>(new KeyInt32 { Name = "Some Name" });

                    // Assert
                    id.Should().BeGreaterThan(0);
                }
            }

            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Inserts_entity_with_int64_primary_key(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Act
                    var id = database.Insert<int>(new KeyInt64 { Name = "Some Name" });

                    // Assert
                    id.Should().BeGreaterThan(0);
                }
            }

            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Uses_key_attribute_to_determine_key(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Act
                    var id = database.Insert<int>(new KeyAlias { Name = "Some Name" });

                    // Assert
                    id.Should().BeGreaterThan(0);
                }
            }

            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Inserts_into_other_schemas(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Act
                    var id = database.Insert<int>(new SchemaOther { Name = "Some name" });

                    // Assert
                    id.Should().BeGreaterThan(0);
                }
            }
        }

        public class InsertRange
            : DefaultDatabaseConnectionCrudTests
        {
            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Inserts_entity_with_int32_key(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var entities = new[]
                        {
                            new KeyInt32 { Name = "Some Name" },
                            new KeyInt32 { Name = "Some Name2" }
                        };

                    // Act
                    database.InsertRange(entities);

                    // Assert
                    database.Count<KeyInt32>().Should().Be(2);
                }
            }

            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Inserts_entity_with_int64_key(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var entities = new[]
                        {
                            new KeyInt64 { Name = "Some Name" },
                            new KeyInt64 { Name = "Some Name2" }
                        };

                    // Act
                    database.InsertRange(entities);

                    // Assert
                    database.Count<KeyInt64>().Should().Be(2);
                }
            }

            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Inserts_entities_with_composite_keys(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var entities = new[]
                        {
                            new CompositeKeys { Key1 = 2, Key2 = 3, Name = "Some Name1" },
                            new CompositeKeys { Key1 = 3, Key2 = 3, Name = "Some Name2" }
                        };

                    // Act
                    database.InsertRange(entities);

                    // Assert
                    database.Count<CompositeKeys>().Should().Be(2);
                }
            }

            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Inserts_entities_with_string_key(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var entities = new[]
                        {
                            new KeyString { Name = "Some Name", Age = 10 },
                            new KeyString { Name = "Some Name2", Age = 11 }
                        };

                    // Act
                    database.InsertRange(entities);

                    // Assert
                    database.Count<KeyString>().Should().Be(2);
                }
            }

            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Inserts_entities_with_guid_key(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var entities = new[]
                        {
                            new KeyGuid { Id = Guid.NewGuid(), Name = "Some Name" },
                            new KeyGuid { Id = Guid.NewGuid(), Name = "Some Name2" }
                        };

                    // Act
                    database.InsertRange(entities);

                    // Assert
                    database.Count<KeyGuid>().Should().Be(2);
                }
            }

            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Uses_key_attribute_to_determine_key(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange

                    var entities = new[]
                        {
                            new KeyAlias { Name = "Some Name" },
                            new KeyAlias { Name = "Some Name2" }
                        };

                    // Act
                    database.InsertRange(entities);

                    // Assert
                    database.Count<KeyAlias>().Should().Be(2);
                }
            }

            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Inserts_into_other_schemas(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var entities = new[]
                        {
                            new SchemaOther { Name = "Some Name" },
                            new SchemaOther { Name = "Some Name2" }

                        };

                    // Act
                    database.InsertRange(entities);

                    // Assert
                    database.Count<SchemaOther>().Should().Be(2);
                }
            }
        }

        public class InsertRangeAndSetKey
            : DefaultDatabaseConnectionCrudTests
        {
            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Throws_exception_when_entity_has_no_key(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var entities = new[]
                        {
                            new NoKey()
                        };

                    // Act / Assert
                    Assert.Throws<InvalidPrimaryKeyException>(
                        () => database.InsertRange<NoKey, int>(entities, (e, k) => { }));
                }
            }

            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Throws_exception_when_entity_has_composite_keys(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var entities = new[]
                        {
                            new CompositeKeys()
                        };

                    // Act / Assert
                    Assert.Throws<InvalidPrimaryKeyException>(
                        () => database.InsertRange<CompositeKeys, int>(entities, (e, k) => { }));
                }
            }

            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Throws_exception_for_string_keys(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var entities = new[]
                        {
                            new KeyString { Name = "Some Name", Age = 10 }
                        };

                    // Act / Assert
                    Assert.Throws<InvalidPrimaryKeyException>(
                        () => database.InsertRange<KeyString, string>(entities, (e, k) => { }));
                }
            }

            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Throws_exception_for_guid_keys(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var entities = new[]
                        {
                            new KeyGuid { Id = Guid.NewGuid(), Name = "Some Name" }
                        };

                    // Act / Assert
                    Assert.Throws<InvalidPrimaryKeyException>(
                        () => database.InsertRange<KeyGuid, Guid>(entities, (e, k) => { }));
                }
            }

            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Inserts_entity_with_int32_primary_key(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var entities = new[]
                        {
                            new KeyInt32 { Name = "Some Name" },
                            new KeyInt32 { Name = "Some Name2" },
                            new KeyInt32 { Name = "Some Name3" }
                        };

                    // Act
                    database.InsertRange<KeyInt32, int>(entities, (e, k) => { e.Id = k; });

                    // Assert
                    entities[0].Id.Should().BeGreaterThan(0);
                    entities[1].Id.Should().BeGreaterThan(entities[0].Id);
                    entities[2].Id.Should().BeGreaterThan(entities[1].Id);
                }
            }

            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Inserts_entity_with_int64_primary_key(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var entities = new[]
                        {
                            new KeyInt64 { Name = "Some Name" },
                            new KeyInt64 { Name = "Some Name2" },
                            new KeyInt64 { Name = "Some Name3" }
                        };

                    // Act
                    database.InsertRange<KeyInt64, long>(entities, (e, k) => { e.Id = k; });

                    // Assert
                    entities[0].Id.Should().BeGreaterThan(0);
                    entities[1].Id.Should().BeGreaterThan(entities[0].Id);
                    entities[2].Id.Should().BeGreaterThan(entities[1].Id);
                }
            }

            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Uses_key_attribute_to_determine_key(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var entities = new[]
                        {
                            new KeyExplicit { Name = "Some Name" }
                        };

                    // Act
                    database.InsertRange<KeyExplicit, int>(entities, (e, k) => { e.Key = k; });

                    // Assert
                    entities[0].Key.Should().BeGreaterThan(0);
                }
            }

            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Inserts_into_other_schemas(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var entities = new[]
                        {
                            new SchemaOther { Name = "Some Name" },
                            new SchemaOther { Name = "Some Name2" },
                            new SchemaOther { Name = "Some Name3" }
                        };

                    // Act
                    database.InsertRange<SchemaOther, int>(entities, (e, k) => { e.Id = k; });

                    // Assert
                    entities[0].Id.Should().BeGreaterThan(0);
                    entities[1].Id.Should().BeGreaterThan(entities[0].Id);
                    entities[2].Id.Should().BeGreaterThan(entities[1].Id);
                }
            }
        }
    }
}