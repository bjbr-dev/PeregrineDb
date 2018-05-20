namespace PeregrineDb.Tests.Databases
{
    using System;
    using System.Diagnostics.CodeAnalysis;
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
        public class InsertAsync
            : DefaultDatabaseConnectionCrudAsyncTests
        {
            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Inserts_entity_with_int32_key(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var entity = new KeyInt32 { Name = "Some Name" };

                    // Act
                    await database.InsertAsync(entity);

                    // Assert
                    database.Count<KeyInt32>().Should().Be(1);
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Inserts_entity_with_int64_key(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var entity = new KeyInt64 { Name = "Some Name" };

                    // Act
                    await database.InsertAsync(entity);

                    // Assert
                    database.Count<KeyInt64>().Should().Be(1);
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Inserts_entities_with_composite_keys(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var entity = new CompositeKeys { Key1 = 2, Key2 = 3, Name = "Some Name" };

                    // Act
                    await database.InsertAsync(entity);

                    // Assert
                    database.Count<CompositeKeys>().Should().Be(1);
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Does_not_allow_part_of_composite_key_to_be_null(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var entity = new CompositeKeys { Key1 = null, Key2 = 5, Name = "Some Name" };

                    // Act
                    Func<Task> act = async () => await database.InsertAsync(entity);

                    // Assert
                    act.ShouldThrow<Exception>();
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Inserts_entities_with_string_key(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var entity = new KeyString { Name = "Some Name", Age = 10 };

                    // Act
                    await database.InsertAsync(entity);

                    // Assert
                    database.Count<KeyString>().Should().Be(1);
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Does_not_allow_string_key_to_be_null(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var entity = new KeyString { Name = null, Age = 10 };

                    // Act
                    Func<Task> act = async () => await database.InsertAsync(entity);

                    // Assert
                    act.ShouldThrow<Exception>();
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Inserts_entities_with_guid_key(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var entity = new KeyGuid { Id = Guid.NewGuid(), Name = "Some Name" };

                    // Act
                    await database.InsertAsync(entity);

                    // Assert
                    database.Count<KeyGuid>().Should().Be(1);
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Uses_key_attribute_to_determine_key(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var entity = new KeyAlias { Name = "Some Name" };

                    // Act
                    await database.InsertAsync(entity);

                    // Assert
                    database.Count<KeyAlias>().Should().Be(1);
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Inserts_into_other_schemas(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var entity = new SchemaOther { Name = "Some name" };

                    // Act
                    await database.InsertAsync(entity);

                    // Assert
                    database.Count<SchemaOther>().Should().Be(1);
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Ignores_columns_which_are_not_mapped(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var entity = new PropertyNotMapped { FirstName = "Bobby", LastName = "DropTables", Age = 10 };

                    // Act
                    await database.InsertAsync(entity);

                    // Assert
                    database.Count<PropertyNotMapped>().Should().Be(1);
                }
            }
        }

        public class InsertAndReturnKeyAsync
            : DefaultDatabaseConnectionCrudAsyncTests
        {
            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Throws_exception_when_entity_has_no_key(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Act
                    Func<Task> act = async () => await database.InsertAsync<int>(new NoKey());

                    // Assert
                    act.ShouldThrow<InvalidPrimaryKeyException>();
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Throws_exception_when_entity_has_composite_keys(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Act
                    Func<Task> act = async () => await database.InsertAsync<int>(new CompositeKeys());

                    // Assert
                    act.ShouldThrow<InvalidPrimaryKeyException>();
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

                    // Act
                    Func<Task> act = async () => await database.InsertAsync<string>(entity);

                    // Assert
                    act.ShouldThrow<InvalidPrimaryKeyException>();
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

                    // Act
                    Func<Task> act = async () => await database.InsertAsync<Guid>(entity);

                    // Assert
                    act.ShouldThrow<InvalidPrimaryKeyException>();
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Inserts_entity_with_int32_primary_key(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Act
                    var id = await database.InsertAsync<int>(new KeyInt32 { Name = "Some Name" });

                    // Assert
                    id.Should().BeGreaterThan(0);
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Inserts_entity_with_int64_primary_key(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Act
                    var id = await database.InsertAsync<int>(new KeyInt64 { Name = "Some Name" });

                    // Assert
                    id.Should().BeGreaterThan(0);
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Uses_key_attribute_to_determine_key(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Act
                    var id = await database.InsertAsync<int>(new KeyAlias { Name = "Some Name" });

                    // Assert
                    id.Should().BeGreaterThan(0);
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Inserts_into_other_schemas(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Act
                    var id = await database.InsertAsync<int>(new SchemaOther { Name = "Some name" });

                    // Assert
                    id.Should().BeGreaterThan(0);
                }
            }
        }

        public class InsertRangeAsync
            : DefaultDatabaseConnectionCrudAsyncTests
        {
            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Inserts_entity_with_int32_key(IDialect dialect)
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
                    await database.InsertRangeAsync(entities);

                    // Assert
                    database.Count<KeyInt32>().Should().Be(2);
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Inserts_entity_with_int64_key(IDialect dialect)
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
                    await database.InsertRangeAsync(entities);

                    // Assert
                    database.Count<KeyInt64>().Should().Be(2);
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Inserts_entities_with_composite_keys(IDialect dialect)
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
                    await database.InsertRangeAsync(entities);

                    // Assert
                    database.Count<CompositeKeys>().Should().Be(2);
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Inserts_entities_with_string_key(IDialect dialect)
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
                    await database.InsertRangeAsync(entities);

                    // Assert
                    database.Count<KeyString>().Should().Be(2);
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Inserts_entities_with_guid_key(IDialect dialect)
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
                    await database.InsertRangeAsync(entities);

                    // Assert
                    database.Count<KeyGuid>().Should().Be(2);
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Uses_key_attribute_to_determine_key(IDialect dialect)
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
                    await database.InsertRangeAsync(entities);

                    // Assert
                    database.Count<KeyAlias>().Should().Be(2);
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Inserts_into_other_schemas(IDialect dialect)
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
                    await database.InsertRangeAsync(entities);

                    // Assert
                    database.Count<SchemaOther>().Should().Be(2);
                }
            }
        }

        public class InsertRangeAndSetKeyAsync
            : DefaultDatabaseConnectionCrudAsyncTests
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

                    // Act
                    Func<Task> act = async () => await database.InsertRangeAsync<NoKey, int>(entities, (e, k) => { });

                    // Assert
                    act.ShouldThrow<InvalidPrimaryKeyException>();
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

                    // Act
                    Func<Task> act = async () => await database.InsertRangeAsync<CompositeKeys, int>(entities, (e, k) => { });

                    // Assert
                    act.ShouldThrow<InvalidPrimaryKeyException>();
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

                    // Act
                    Func<Task> act = async () => await database.InsertRangeAsync<KeyString, string>(entities, (e, k) => { });

                    // Assert
                    act.ShouldThrow<InvalidPrimaryKeyException>();
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

                    // Act
                    Func<Task> act = async () => await database.InsertRangeAsync<KeyGuid, Guid>(entities, (e, k) => { });

                    // Assert
                    act.ShouldThrow<InvalidPrimaryKeyException>();
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Inserts_entity_with_int32_primary_key(IDialect dialect)
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
                    await database.InsertRangeAsync<KeyInt32, int>(entities, (e, k) => { e.Id = k; });

                    // Assert
                    entities[0].Id.Should().BeGreaterThan(0);
                    entities[1].Id.Should().BeGreaterThan(entities[0].Id);
                    entities[2].Id.Should().BeGreaterThan(entities[1].Id);
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Inserts_entity_with_int64_primary_key(IDialect dialect)
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
                    await database.InsertRangeAsync<KeyInt64, long>(entities, (e, k) => { e.Id = k; });

                    // Assert
                    entities[0].Id.Should().BeGreaterThan(0);
                    entities[1].Id.Should().BeGreaterThan(entities[0].Id);
                    entities[2].Id.Should().BeGreaterThan(entities[1].Id);
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Uses_key_attribute_to_determine_key(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var entities = new[]
                        {
                            new KeyExplicit { Name = "Some Name" }
                        };

                    // Act
                    await database.InsertRangeAsync<KeyExplicit, int>(entities, (e, k) => { e.Key = k; });

                    // Assert
                    entities[0].Key.Should().BeGreaterThan(0);
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Inserts_into_other_schemas(IDialect dialect)
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
                    await database.InsertRangeAsync<SchemaOther, int>(entities, (e, k) => { e.Id = k; });

                    // Assert
                    entities[0].Id.Should().BeGreaterThan(0);
                    entities[1].Id.Should().BeGreaterThan(entities[0].Id);
                    entities[2].Id.Should().BeGreaterThan(entities[1].Id);
                }
            }
        }
    }
}