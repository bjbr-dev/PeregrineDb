namespace PeregrineDb.Tests.Databases
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using FluentAssertions;
    using Pagination;
    using PeregrineDb.Dialects;
    using PeregrineDb.Schema;
    using PeregrineDb.Tests.ExampleEntities;
    using PeregrineDb.Tests.Utils;
    using Xunit;

    public class DefaultDatabaseConnectionCrudAsyncTests
    {
        private static IEnumerable<IDialect> TestDialects => new[]
            {
                Dialect.SqlServer2012,
                Dialect.PostgreSql
            };

        public abstract class CountAsync
            : DefaultDatabaseConnectionCrudAsyncTests
        {
            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Counts_entities(IDialect dialect)
            {
                using (var instance = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var database = instance.Item;
                    database.Insert(new User { Name = "Some Name 1", Age = 10 });
                    database.Insert(new User { Name = "Some Name 2", Age = 10 });
                    database.Insert(new User { Name = "Some Name 3", Age = 10 });
                    database.Insert(new User { Name = "Some Name 4", Age = 11 });

                    // Act
                    var result = await database.CountAsync<User>();

                    // Assert
                    result.Should().Be(4);
                }
            }

            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Counts_entities_matching_conditions(IDialect dialect)
            {
                using (var instance = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var database = instance.Item;
                    database.Insert(new User { Name = "Some Name 1", Age = 10 });
                    database.Insert(new User { Name = "Some Name 2", Age = 10 });
                    database.Insert(new User { Name = "Some Name 3", Age = 10 });
                    database.Insert(new User { Name = "Some Name 4", Age = 11 });

                    // Act
                    var result = await database.CountAsync<User>(
                        "WHERE Age < @Age",
                        new { Age = 11 });

                    // Assert
                    result.Should().Be(3);

                    // Cleanup
                    database.DeleteAll<User>();
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Counts_entities_in_alternate_schema(IDialect dialect)
            {
                using (var instance = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var database = instance.Item;
                    database.Insert<int>(new SchemaOther { Name = "Some Name" });
                    database.Insert<int>(new SchemaOther { Name = "Some Name" });
                    database.Insert<int>(new SchemaOther { Name = "Some Name" });
                    database.Insert<int>(new SchemaOther { Name = "Some Name" });

                    // Act
                    var result = await database.CountAsync<SchemaOther>();

                    // Assert
                    result.Should().Be(4);

                    // Cleanup
                    database.DeleteAll<SchemaOther>();
                }
            }
        }

        public abstract class CountAsyncWhereObject
            : DefaultDatabaseConnectionCrudAsyncTests
        {
            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Throws_exception_when_conditions_is_null(IDialect dialect)
            {
                using (var instance = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var database = instance.Item;
                    // Act
                    Func<Task> act = async () => await database.CountAsync<User>((object)null);

                    // Assert
                    act.ShouldThrow<ArgumentNullException>();
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Counts_all_entities_when_conditions_is_empty(IDialect dialect)
            {
                using (var instance = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var database = instance.Item;
                    database.Insert(new User { Name = "Some Name 1", Age = 10 });
                    database.Insert(new User { Name = "Some Name 2", Age = 10 });
                    database.Insert(new User { Name = "Some Name 3", Age = 10 });
                    database.Insert(new User { Name = "Some Name 4", Age = 11 });

                    // Act
                    var result = await database.CountAsync<User>(new { });

                    // Assert
                    result.Should().Be(4);

                    // Cleanup
                    database.DeleteAll<User>();
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Counts_entities_matching_conditions(IDialect dialect)
            {
                using (var instance = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var database = instance.Item;
                    database.Insert(new User { Name = "Some Name 1", Age = 10 });
                    database.Insert(new User { Name = "Some Name 2", Age = 10 });
                    database.Insert(new User { Name = "Some Name 3", Age = 10 });
                    database.Insert(new User { Name = "Some Name 4", Age = 11 });

                    // Act
                    var result = await database.CountAsync<User>(new { Age = 10 });

                    // Assert
                    result.Should().Be(3);

                    // Cleanup
                    database.DeleteAll<User>();
                }
            }
        }

        public abstract class FindAsync
            : DefaultDatabaseConnectionCrudAsyncTests
        {
            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Throws_exception_when_entity_has_no_key(IDialect dialect)
            {
                using (var instance = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var database = instance.Item;
                    database.Insert(new NoKey { Name = "Some Name", Age = 1 });

                    // Act
                    Func<Task> act = async () => await database.FindAsync<NoKey>("Some Name");

                    // Assert
                    act.ShouldThrow<InvalidPrimaryKeyException>();
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Returns_null_when_entity_is_not_found(IDialect dialect)
            {
                using (var instance = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var database = instance.Item;
                    // Act
                    var entity = await database.FindAsync<KeyInt32>(12);

                    // Assert
                    entity.Should().Be(null);
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Finds_entity_by_Int32_primary_key(IDialect dialect)
            {
                using (var instance = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var database = instance.Item;
                    var id = database.Insert<int>(new KeyInt32 { Name = "Some Name" });

                    // Act
                    var entity = await database.FindAsync<KeyInt32>(id);

                    // Assert
                    entity.Name.Should().Be("Some Name");

                    // Cleanup
                    database.Delete<KeyInt32>(id);
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Finds_entity_by_Int64_primary_key(IDialect dialect)
            {
                using (var instance = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var database = instance.Item;
                    var id = database.Insert<long>(new KeyInt64 { Name = "Some Name" });

                    // Act
                    var user = await database.FindAsync<KeyInt64>(id);

                    // Assert
                    user.Name.Should().Be("Some Name");

                    // Cleanup
                    database.Delete<KeyInt64>(id);
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Finds_entity_by_string_primary_key(IDialect dialect)
            {
                using (var instance = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var database = instance.Item;
                    database.Insert(new KeyString { Name = "Some Name", Age = 42 });

                    // Act
                    var entity = await database.FindAsync<KeyString>("Some Name");

                    // Assert
                    entity.Age.Should().Be(42);

                    // Cleanup
                    database.Delete(entity);
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Finds_entity_by_guid_primary_key(IDialect dialect)
            {
                using (var instance = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var database = instance.Item;
                    var id = Guid.NewGuid();
                    database.Insert(new KeyGuid { Id = id, Name = "Some Name" });

                    // Act
                    var entity = await database.FindAsync<KeyGuid>(id);

                    // Assert
                    entity.Name.Should().Be("Some Name");

                    // Cleanup
                    database.Delete(entity);
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Finds_entity_by_composite_key(IDialect dialect)
            {
                using (var instance = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var database = instance.Item;
                    database.Insert(new CompositeKeys { Key1 = 1, Key2 = 1, Name = "Some Name" });
                    var id = new { Key1 = 1, Key2 = 1 };

                    // Act
                    var entity = await database.FindAsync<CompositeKeys>(id);

                    // Assert
                    entity.Name.Should().Be("Some Name");

                    // Cleanup
                    database.DeleteAll<CompositeKeys>();
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Finds_entities_in_alternate_schema(IDialect dialect)
            {
                using (var instance = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var database = instance.Item;
                    var id = database.Insert<int>(new SchemaOther { Name = "Some Name" });

                    // Act
                    var entity = await database.FindAsync<SchemaOther>(id);

                    // Assert
                    entity.Name.Should().Be("Some Name");

                    // Cleanup
                    database.Delete<SchemaOther>(id);
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Finds_entities_with_enum_property(IDialect dialect)
            {
                using (var instance = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var database = instance.Item;
                    var id = database.Insert<int>(new PropertyEnum { FavoriteColor = Color.Green });

                    // Act
                    var entity = await database.FindAsync<PropertyEnum>(id);

                    // Assert
                    entity.FavoriteColor.Should().Be(Color.Green);

                    // Cleanup
                    database.Delete<PropertyEnum>(id);
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Finds_entities_with_all_possible_types(IDialect dialect)
            {
                using (var instance = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var database = instance.Item;
                    var id = database.Insert<int>(
                        new PropertyAllPossibleTypes
                            {
                                Int16Property = -16,
                                NullableInt16Property = -16,
                                Int32Property = -32,
                                NullableInt32Property = -32,
                                Int64Property = -64,
                                NullableInt64Property = -64,
                                SingleProperty = 1,
                                NullableSingleProperty = 1,
                                DoubleProperty = 2,
                                NullableDoubleProperty = 2,
                                DecimalProperty = 10,
                                NullableDecimalProperty = 10,
                                BoolProperty = true,
                                NullableBoolProperty = true,
                                StringProperty = "Foo",
                                CharProperty = 'F',
                                NullableCharProperty = 'N',
                                GuidProperty = new Guid("da8326a1-c703-4a79-9fb2-2909b0f40367"),
                                NullableGuidProperty = new Guid("706e6bcf-4a6d-4d19-91e9-935852140c4d"),
                                DateTimeProperty = new DateTime(2016, 12, 31),
                                NullableDateTimeProperty = new DateTime(2016, 12, 31),
                                DateTimeOffsetProperty = new DateTimeOffset(new DateTime(2016, 12, 31), new TimeSpan(0, 1, 0, 0)),
                                NullableDateTimeOffsetProperty = new DateTimeOffset(new DateTime(2016, 12, 31), new TimeSpan(0, 1, 0, 0)),
                                ByteArrayProperty = new byte[] { 1, 2, 3 }
                            });

                    // Act
                    var entity = await database.FindAsync<PropertyAllPossibleTypes>(id);

                    // Assert
                    entity.Int16Property.Should().Be(-16);
                    entity.NullableInt16Property.Should().Be(-16);
                    entity.Int32Property.Should().Be(-32);
                    entity.NullableInt32Property.Should().Be(-32);
                    entity.Int64Property.Should().Be(-64);
                    entity.NullableInt64Property.Should().Be(-64);
                    entity.SingleProperty.Should().Be(1);
                    entity.NullableSingleProperty.Should().Be(1);
                    entity.DoubleProperty.Should().Be(2);
                    entity.NullableDoubleProperty.Should().Be(2);
                    entity.DecimalProperty.Should().Be(10);
                    entity.NullableDecimalProperty.Should().Be(10);
                    entity.BoolProperty.Should().Be(true);
                    entity.NullableBoolProperty.Should().Be(true);
                    entity.StringProperty.Should().Be("Foo");
                    entity.CharProperty.Should().Be('F');
                    entity.NullableCharProperty.Should().Be('N');
                    entity.GuidProperty.Should().Be(new Guid("da8326a1-c703-4a79-9fb2-2909b0f40367"));
                    entity.NullableGuidProperty.Should().Be(new Guid("706e6bcf-4a6d-4d19-91e9-935852140c4d"));
                    entity.DateTimeProperty.Should().Be(new DateTime(2016, 12, 31));
                    entity.NullableDateTimeProperty.Should().Be(new DateTime(2016, 12, 31));
                    entity.DateTimeOffsetProperty.Should().Be(new DateTimeOffset(new DateTime(2016, 12, 31), new TimeSpan(0, 1, 0, 0)));
                    entity.NullableDateTimeOffsetProperty.Should().Be(new DateTimeOffset(new DateTime(2016, 12, 31), new TimeSpan(0, 1, 0, 0)));
                    entity.ByteArrayProperty.ShouldAllBeEquivalentTo(new byte[] { 1, 2, 3 }, o => o.WithStrictOrdering());

                    // Cleanup
                    database.Delete<PropertyAllPossibleTypes>(id);
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Ignores_columns_which_are_not_mapped(IDialect dialect)
            {
                using (var instance = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var database = instance.Item;
                    var id = database.Insert<int>(new PropertyNotMapped { Firstname = "Bobby", LastName = "DropTables", Age = 10 });

                    // Act
                    var entity = await database.FindAsync<PropertyNotMapped>(id);

                    // Assert
                    entity.Firstname.Should().Be("Bobby");
                    entity.LastName.Should().Be("DropTables");
                    entity.FullName.Should().Be("Bobby DropTables");
                    entity.Age.Should().Be(0);

                    // Cleanup
                    database.DeleteAll<PropertyNotMapped>();
                }
            }
        }

        public abstract class GetAsync
            : DefaultDatabaseConnectionCrudAsyncTests
        {
            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Throws_exception_when_entity_has_no_key(IDialect dialect)
            {
                using (var instance = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var database = instance.Item;
                    database.Insert(new NoKey { Name = "Some Name", Age = 1 });

                    // Act
                    Func<Task> act = async () => await database.GetAsync<NoKey>("Some Name");

                    // Assert
                    act.ShouldThrow<InvalidPrimaryKeyException>();
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Throws_exception_when_entity_is_not_found(IDialect dialect)
            {
                using (var instance = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var database = instance.Item;
                    Func<Task> act = async () => await database.GetAsync<KeyInt32>(5);

                    // Assert
                    act.ShouldThrow<InvalidOperationException>();
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Finds_entity_by_Int32_primary_key(IDialect dialect)
            {
                using (var instance = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var database = instance.Item;
                    var id = database.Insert<int>(new KeyInt32 { Name = "Some Name" });

                    // Act
                    var entity = await database.GetAsync<KeyInt32>(id);

                    // Assert
                    entity.Name.Should().Be("Some Name");

                    // Cleanup
                    database.Delete<KeyInt32>(id);
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Finds_entity_by_Int64_primary_key(IDialect dialect)
            {
                using (var instance = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var database = instance.Item;
                    var id = database.Insert<long>(new KeyInt64 { Name = "Some Name" });

                    // Act
                    var entity = await database.GetAsync<KeyInt64>(id);

                    // Assert
                    entity.Name.Should().Be("Some Name");

                    // Cleanup
                    database.Delete<KeyInt64>(id);
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Finds_entity_by_string_primary_key(IDialect dialect)
            {
                using (var instance = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var database = instance.Item;
                    database.Insert(new KeyString { Name = "Some Name", Age = 42 });

                    // Act
                    var entity = await database.GetAsync<KeyString>("Some Name");

                    // Assert
                    entity.Age.Should().Be(42);

                    // Cleanup
                    database.Delete(entity);
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Finds_entity_by_guid_primary_key(IDialect dialect)
            {
                using (var instance = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var database = instance.Item;
                    var id = Guid.NewGuid();
                    database.Insert(new KeyGuid { Id = id, Name = "Some Name" });

                    // Act
                    var entity = await database.GetAsync<KeyGuid>(id);

                    // Assert
                    entity.Name.Should().Be("Some Name");

                    // Cleanup
                    database.Delete(entity);
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Finds_entity_by_composite_key(IDialect dialect)
            {
                using (var instance = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var database = instance.Item;
                    database.Insert(new CompositeKeys { Key1 = 1, Key2 = 1, Name = "Some Name" });
                    var id = new { Key1 = 1, Key2 = 1 };

                    // Act
                    var entity = await database.GetAsync<CompositeKeys>(id);

                    // Assert
                    entity.Name.Should().Be("Some Name");

                    // Cleanup
                    database.DeleteAll<CompositeKeys>();
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Finds_entities_in_alternate_schema(IDialect dialect)
            {
                using (var instance = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var database = instance.Item;
                    var id = database.Insert<int>(new SchemaOther { Name = "Some Name" });

                    // Act
                    var entity = await database.GetAsync<SchemaOther>(id);

                    // Assert
                    entity.Name.Should().Be("Some Name");

                    // Cleanup
                    database.Delete<SchemaOther>(id);
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Ignores_columns_which_are_not_mapped(IDialect dialect)
            {
                using (var instance = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var database = instance.Item;
                    var id = database.Insert<int>(new PropertyNotMapped { Firstname = "Bobby", LastName = "DropTables", Age = 10 });

                    // Act
                    var entity = await database.GetAsync<PropertyNotMapped>(id);

                    // Assert
                    entity.Firstname.Should().Be("Bobby");
                    entity.LastName.Should().Be("DropTables");
                    entity.FullName.Should().Be("Bobby DropTables");
                    entity.Age.Should().Be(0);

                    // Cleanup
                    database.DeleteAll<PropertyNotMapped>();
                }
            }
        }

        public abstract class GetRangeAsync
            : DefaultDatabaseConnectionCrudAsyncTests
        {
            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Filters_result_by_conditions(IDialect dialect)
            {
                using (var instance = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var database = instance.Item;
                    database.Insert(new User { Name = "Some Name 1", Age = 10 });
                    database.Insert(new User { Name = "Some Name 2", Age = 10 });
                    database.Insert(new User { Name = "Some Name 3", Age = 10 });
                    database.Insert(new User { Name = "Some Name 4", Age = 11 });

                    // Act
                    var users = await database.GetRangeAsync<User>(
                        "WHERE Name LIKE CONCAT(@Search, '%') and Age = @Age",
                        new { Search = "Some Name", Age = 10 });

                    // Assert
                    users.Should().HaveCount(3);

                    // Cleanup
                    database.DeleteAll<User>();
                }
            }

            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Returns_everything_when_conditions_is_null(IDialect dialect)
            {
                using (var instance = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var database = instance.Item;
                    database.Insert(new User { Name = "Some Name 1", Age = 10 });
                    database.Insert(new User { Name = "Some Name 2", Age = 10 });
                    database.Insert(new User { Name = "Some Name 3", Age = 10 });
                    database.Insert(new User { Name = "Some Name 4", Age = 11 });

                    // Act
                    var users = await database.GetRangeAsync<User>(null);

                    // Assert
                    users.Count().Should().Be(4);

                    // Cleanup
                    database.DeleteAll<User>();
                }
            }
        }

        public abstract class GetRangeAsyncWhereObject
            : DefaultDatabaseConnectionCrudAsyncTests
        {
            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Throws_exception_when_conditions_is_null(IDialect dialect)
            {
                using (var instance = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var database = instance.Item;
                    // Act
                    Func<Task> act = async () => await database.GetRangeAsync<User>((object)null);

                    // Assert
                    act.ShouldThrow<ArgumentNullException>();
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Returns_all_when_conditions_is_empty(IDialect dialect)
            {
                using (var instance = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var database = instance.Item;
                    // Arrange
                    database.Insert(new User { Name = "Some Name 1", Age = 10 });
                    database.Insert(new User { Name = "Some Name 2", Age = 10 });
                    database.Insert(new User { Name = "Some Name 3", Age = 10 });
                    database.Insert(new User { Name = "Some Name 4", Age = 11 });

                    // Act
                    var users = await database.GetRangeAsync<User>(new { });

                    // Assert
                    users.Count().Should().Be(4);

                    // Cleanup
                    database.DeleteAll<User>();
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Filters_result_by_conditions(IDialect dialect)
            {
                using (var instance = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var database = instance.Item;
                    // Arrange
                    database.Insert(new User { Name = "Some Name 1", Age = 10 });
                    database.Insert(new User { Name = "Some Name 2", Age = 10 });
                    database.Insert(new User { Name = "Some Name 3", Age = 10 });
                    database.Insert(new User { Name = "Some Name 4", Age = 11 });

                    // Act
                    var users = await database.GetRangeAsync<User>(new { Age = 10 });

                    // Assert
                    users.Count().Should().Be(3);

                    // Cleanup
                    database.DeleteAll<User>();
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Matches_column_name_case_insensitively(IDialect dialect)
            {
                using (var instance = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var database = instance.Item;
                    database.Insert(new User { Name = "Some Name 1", Age = 10 });
                    database.Insert(new User { Name = "Some Name 2", Age = 10 });
                    database.Insert(new User { Name = "Some Name 3", Age = 10 });
                    database.Insert(new User { Name = "Some Name 4", Age = 11 });

                    // Act
                    var users = await database.GetRangeAsync<User>(new { age = 10 });

                    // Assert
                    users.Count().Should().Be(3);

                    // Cleanup
                    database.DeleteAll<User>();
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Throws_exception_when_column_not_found(IDialect dialect)
            {
                using (var instance = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var database = instance.Item;
                    // Act
                    Func<Task> act = async () => await database.GetRangeAsync<User>(new { Ages = 10 });

                    // Assert
                    act.ShouldThrow<InvalidConditionSchemaException>();
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task When_value_is_not_null_does_not_find_nulls(IDialect dialect)
            {
                using (var instance = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var database = instance.Item;
                    database.Insert(new PropertyNullable { Name = null });
                    database.Insert(new PropertyNullable { Name = "Some Name 3" });
                    database.Insert(new PropertyNullable { Name = null });

                    // Act
                    var entities = await database.GetRangeAsync<PropertyNullable>(new { Name = "Some Name 3" });

                    // Assert
                    entities.Count().Should().Be(1);

                    // Cleanup
                    database.DeleteAll<PropertyNullable>();
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task When_value_is_null_finds_nulls(IDialect dialect)
            {
                using (var instance = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var database = instance.Item;
                    database.Insert(new PropertyNullable { Name = null });
                    database.Insert(new PropertyNullable { Name = "Some Name 3" });
                    database.Insert(new PropertyNullable { Name = null });

                    // Act
                    var entities = await database.GetRangeAsync<PropertyNullable>(new { Name = (string)null });

                    // Assert
                    entities.Count().Should().Be(2);

                    // Cleanup
                    database.DeleteAll<PropertyNullable>();
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Filters_on_multiple_properties(IDialect dialect)
            {
                using (var instance = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var database = instance.Item;
                    database.Insert(new User { Name = "Some Name 1", Age = 10 });
                    database.Insert(new User { Name = "Some Name 2", Age = 10 });
                    database.Insert(new User { Name = "Some Name 2", Age = 12 });

                    // Act
                    var users = await database.GetRangeAsync<User>(new { Name = "Some Name 2", Age = 10 });

                    // Assert
                    users.Count().Should().Be(1);

                    // Cleanup
                    database.DeleteAll<User>();
                }
            }
        }

        public abstract class GetPageAsync
            : DefaultDatabaseConnectionCrudAsyncTests
        {
            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Returns_empty_list_when_there_are_no_entities(IDialect dialect)
            {
                using (var instance = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var database = instance.Item;
                    // Act
                    var pageBuilder = new PageIndexPageBuilder(1, 10);
                    var users = await database.GetPageAsync<User>(pageBuilder, null, "Age");

                    // Assert
                    users.Items.Count().Should().Be(0);
                }
            }

            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Filters_result_by_conditions(IDialect dialect)
            {
                using (var instance = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var database = instance.Item;
                    database.Insert(new User { Name = "Some Name 1", Age = 10 });
                    database.Insert(new User { Name = "Some Name 2", Age = 10 });
                    database.Insert(new User { Name = "Some Name 3", Age = 10 });
                    database.Insert(new User { Name = "Some Name 4", Age = 11 });

                    // Act
                    var users = await database.GetPageAsync<User>(
                        new PageIndexPageBuilder(1, 10),
                        "WHERE Name LIKE CONCAT(@Search, '%') and Age = @Age",
                        "Age",
                        new { Search = "Some Name", Age = 10 });

                    // Assert
                    users.Items.Count().Should().Be(3);

                    // Cleanup
                    database.DeleteAll<User>();
                }
            }

            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Gets_first_page(IDialect dialect)
            {
                using (var instance = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var database = instance.Item;
                    database.Insert(new User { Name = "Some Name 1", Age = 10 });
                    database.Insert(new User { Name = "Some Name 2", Age = 10 });
                    database.Insert(new User { Name = "Some Name 3", Age = 10 });
                    database.Insert(new User { Name = "Some Name 4", Age = 11 });

                    // Act
                    var users = (await database.GetPageAsync<User>(
                        new PageIndexPageBuilder(1, 2),
                        "WHERE Name LIKE CONCAT(@Search, '%') and Age = @Age",
                        "Age DESC",
                        new { Search = "Some Name", Age = 10 })).Items;

                    // Assert
                    users.Count().Should().Be(2);
                    users[0].Name.Should().Be("Some Name 1");
                    users[1].Name.Should().Be("Some Name 2");

                    // Cleanup
                    database.DeleteAll<User>();
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Gets_second_page(IDialect dialect)
            {
                using (var instance = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var database = instance.Item;
                    database.Insert(new User { Name = "Some Name 1", Age = 10 });
                    database.Insert(new User { Name = "Some Name 2", Age = 10 });
                    database.Insert(new User { Name = "Some Name 3", Age = 10 });
                    database.Insert(new User { Name = "Some Name 4", Age = 11 });

                    // Act
                    var users = (await database.GetPageAsync<User>(
                        new PageIndexPageBuilder(2, 2),
                        "WHERE Name LIKE CONCAT(@Search, '%') and Age = @Age",
                        "Age DESC",
                        new { Search = "Some Name", Age = 10 })).Items;

                    // Assert
                    users.Count().Should().Be(1);
                    users[0].Name.Should().Be("Some Name 3");

                    // Cleanup
                    database.DeleteAll<User>();
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Returns_empty_set_past_last_page(IDialect dialect)
            {
                using (var instance = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var database = instance.Item;
                    database.Insert(new User { Name = "Some Name 1", Age = 10 });
                    database.Insert(new User { Name = "Some Name 2", Age = 10 });
                    database.Insert(new User { Name = "Some Name 3", Age = 10 });
                    database.Insert(new User { Name = "Some Name 4", Age = 11 });

                    // Act
                    var users = (await database.GetPageAsync<User>(
                        new PageIndexPageBuilder(3, 2),
                        "WHERE Name LIKE CONCAT(@Search, '%') and Age = @Age",
                        "Age DESC",
                        new { Search = "Some Name", Age = 10 })).Items;

                    // Assert
                    users.Should().BeEmpty();

                    // Cleanup
                    database.DeleteAll<User>();
                }
            }

            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Returns_page_from_everything_when_conditions_is_null(IDialect dialect)
            {
                using (var instance = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var database = instance.Item;
                    database.Insert(new User { Name = "Some Name 1", Age = 10 });
                    database.Insert(new User { Name = "Some Name 2", Age = 10 });
                    database.Insert(new User { Name = "Some Name 3", Age = 10 });
                    database.Insert(new User { Name = "Some Name 4", Age = 11 });

                    // Act
                    var page = await database.GetPageAsync<User>(new PageIndexPageBuilder(2, 2), null, "Age DESC", new object());
                    var users = page.Items;

                    // Assert
                    users.Count().Should().Be(2);

                    // Cleanup
                    database.DeleteAll<User>();
                }
            }
        }

        public abstract class GetPageAsyncWhereObject
            : DefaultDatabaseConnectionCrudAsyncTests
        {
            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Returns_empty_list_when_there_are_no_entities(IDialect dialect)
            {
                using (var instance = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var database = instance.Item;

                    // Act
                    var pageBuilder = new PageIndexPageBuilder(1, 10);
                    var users = await database.GetPageAsync<User>(pageBuilder, new { Age = 10 }, "Age");

                    // Assert
                    users.Items.Should().BeEmpty();
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Filters_result_by_conditions(IDialect dialect)
            {
                using (var instance = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var database = instance.Item;
                    database.Insert(new User { Name = "Some Name 1", Age = 10 });
                    database.Insert(new User { Name = "Some Name 2", Age = 10 });
                    database.Insert(new User { Name = "Some Name 3", Age = 10 });
                    database.Insert(new User { Name = "Some Name 4", Age = 11 });

                    // Act
                    var pageBuilder = new PageIndexPageBuilder(1, 10);
                    var users = await database.GetPageAsync<User>(pageBuilder, new { Age = 10 }, "Age");

                    // Assert
                    users.Items.Count().Should().Be(3);

                    // Cleanup
                    database.DeleteAll<User>();
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Gets_first_page(IDialect dialect)
            {
                using (var instance = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var database = instance.Item;
                    database.Insert(new User { Name = "Some Name 1", Age = 10 });
                    database.Insert(new User { Name = "Some Name 2", Age = 10 });
                    database.Insert(new User { Name = "Some Name 3", Age = 10 });
                    database.Insert(new User { Name = "Some Name 4", Age = 11 });

                    // Act
                    var pageBuilder = new PageIndexPageBuilder(1, 2);
                    var page = await database.GetPageAsync<User>(pageBuilder, new { Age = 10 }, "Age DESC");
                    var users = page.Items;

                    // Assert
                    users.Count().Should().Be(2);
                    users[0].Name.Should().Be("Some Name 1");
                    users[1].Name.Should().Be("Some Name 2");

                    // Cleanup
                    database.DeleteAll<User>();
                }
            }

            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Gets_second_page(IDialect dialect)
            {
                using (var instance = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var database = instance.Item;
                    database.Insert(new User { Name = "Some Name 1", Age = 10 });
                    database.Insert(new User { Name = "Some Name 2", Age = 10 });
                    database.Insert(new User { Name = "Some Name 3", Age = 10 });
                    database.Insert(new User { Name = "Some Name 4", Age = 11 });

                    // Act
                    var pageBuilder = new PageIndexPageBuilder(2, 2);
                    var page = await database.GetPageAsync<User>(pageBuilder, new { Age = 10 }, "Age DESC");
                    var users = page.Items;

                    // Assert
                    users.Count().Should().Be(1);
                    users[0].Name.Should().Be("Some Name 3");

                    // Cleanup
                    database.DeleteAll<User>();
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Returns_empty_set_past_last_page(IDialect dialect)
            {
                using (var instance = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var database = instance.Item;
                    database.Insert(new User { Name = "Some Name 1", Age = 10 });
                    database.Insert(new User { Name = "Some Name 2", Age = 10 });
                    database.Insert(new User { Name = "Some Name 3", Age = 10 });
                    database.Insert(new User { Name = "Some Name 4", Age = 11 });

                    // Act
                    var pageBuilder = new PageIndexPageBuilder(3, 2);
                    var page = await database.GetPageAsync<User>(pageBuilder, new { Age = 10 }, "Age DESC");
                    var users = page.Items;

                    // Assert
                    users.Should().BeEmpty();

                    // Cleanup
                    database.DeleteAll<User>();
                }
            }
        }

        public abstract class GetAllAsync
            : DefaultDatabaseConnectionCrudAsyncTests
        {
            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Gets_all(IDialect dialect)
            {
                using (var instance = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var database = instance.Item;
                    database.Insert(new User { Name = "Some Name 1", Age = 10 });
                    database.Insert(new User { Name = "Some Name 2", Age = 10 });
                    database.Insert(new User { Name = "Some Name 3", Age = 10 });
                    database.Insert(new User { Name = "Some Name 4", Age = 11 });

                    // Act
                    var users = await database.GetAllAsync<User>();

                    // Assert
                    users.Count().Should().Be(4);

                    // Cleanup
                    database.DeleteAll<User>();
                }
            }
        }

        public abstract class InsertAsync
            : DefaultDatabaseConnectionCrudAsyncTests
        {
            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Inserts_entity_with_int32_key(IDialect dialect)
            {
                using (var instance = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var database = instance.Item;
                    var entity = new KeyInt32 { Name = "Some Name" };

                    // Act
                    await database.InsertAsync(entity);

                    // Assert
                    database.Count<KeyInt32>().Should().Be(1);

                    // Cleanup
                    database.DeleteAll<KeyInt32>();
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Inserts_entity_with_int64_key(IDialect dialect)
            {
                using (var instance = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var database = instance.Item;
                    var entity = new KeyInt64 { Name = "Some Name" };

                    // Act
                    await database.InsertAsync(entity);

                    // Assert
                    database.Count<KeyInt64>().Should().Be(1);

                    // Cleanup
                    database.DeleteAll<KeyInt64>();
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Inserts_entities_with_composite_keys(IDialect dialect)
            {
                using (var instance = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var database = instance.Item;
                    var entity = new CompositeKeys { Key1 = 2, Key2 = 3, Name = "Some Name" };

                    // Act
                    await database.InsertAsync(entity);

                    // Assert
                    database.Count<CompositeKeys>().Should().Be(1);

                    // Cleanup
                    database.DeleteAll<CompositeKeys>();
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Does_not_allow_part_of_composite_key_to_be_null(IDialect dialect)
            {
                using (var instance = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var database = instance.Item;
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
                using (var instance = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var database = instance.Item;
                    var entity = new KeyString { Name = "Some Name", Age = 10 };

                    // Act
                    await database.InsertAsync(entity);

                    // Assert
                    database.Count<KeyString>().Should().Be(1);

                    // Cleanup
                    database.DeleteAll<KeyString>();
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Does_not_allow_string_key_to_be_null(IDialect dialect)
            {
                using (var instance = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var database = instance.Item;
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
                using (var instance = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var database = instance.Item;
                    var entity = new KeyGuid { Id = Guid.NewGuid(), Name = "Some Name" };

                    // Act
                    await database.InsertAsync(entity);

                    // Assert
                    database.Count<KeyGuid>().Should().Be(1);

                    // Cleanup
                    database.DeleteAll<KeyGuid>();
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Uses_key_attribute_to_determine_key(IDialect dialect)
            {
                using (var instance = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var database = instance.Item;
                    var entity = new KeyAlias { Name = "Some Name" };

                    // Act
                    await database.InsertAsync(entity);

                    // Assert
                    database.Count<KeyAlias>().Should().Be(1);

                    // Cleanup
                    database.DeleteAll<KeyAlias>();
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Inserts_into_other_schemas(IDialect dialect)
            {
                using (var instance = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var database = instance.Item;
                    var entity = new SchemaOther { Name = "Some name" };

                    // Act
                    await database.InsertAsync(entity);

                    // Assert
                    database.Count<SchemaOther>().Should().Be(1);

                    // Cleanup
                    database.DeleteAll<SchemaOther>();
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Ignores_columns_which_are_not_mapped(IDialect dialect)
            {
                using (var instance = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var database = instance.Item;
                    var entity = new PropertyNotMapped { Firstname = "Bobby", LastName = "DropTables", Age = 10 };

                    // Act
                    await database.InsertAsync(entity);

                    // Assert
                    database.Count<PropertyNotMapped>().Should().Be(1);

                    // Cleanup
                    database.DeleteAll<PropertyNotMapped>();
                }
            }
        }

        public abstract class InsertAndReturnKeyAsync
            : DefaultDatabaseConnectionCrudAsyncTests
        {
            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Throws_exception_when_entity_has_no_key(IDialect dialect)
            {
                using (var instance = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var database = instance.Item;
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
                using (var instance = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var database = instance.Item;
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
                using (var instance = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var database = instance.Item;
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
                using (var instance = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var database = instance.Item;
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
                using (var instance = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var database = instance.Item;
                    // Act
                    var id = await database.InsertAsync<int>(new KeyInt32 { Name = "Some Name" });

                    // Assert
                    id.Should().BeGreaterThan(0);

                    // Cleanup
                    database.Delete<KeyInt32>(id);
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Inserts_entity_with_int64_primary_key(IDialect dialect)
            {
                using (var instance = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var database = instance.Item;
                    // Act
                    var id = await database.InsertAsync<int>(new KeyInt64 { Name = "Some Name" });

                    // Assert
                    id.Should().BeGreaterThan(0);

                    // Cleanup
                    database.Delete<KeyInt64>(id);
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Uses_key_attribute_to_determine_key(IDialect dialect)
            {
                using (var instance = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var database = instance.Item;
                    // Act
                    var id = await database.InsertAsync<int>(new KeyAlias { Name = "Some Name" });

                    // Assert
                    id.Should().BeGreaterThan(0);

                    // Cleanup
                    database.Delete<KeyAlias>(id);
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Inserts_into_other_schemas(IDialect dialect)
            {
                using (var instance = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var database = instance.Item;
                    // Act
                    var id = await database.InsertAsync<int>(new SchemaOther { Name = "Some name" });

                    // Assert
                    id.Should().BeGreaterThan(0);

                    // Cleanup
                    database.Delete<SchemaOther>(id);
                }
            }
        }

        public abstract class InsertRangeAsync
            : DefaultDatabaseConnectionCrudAsyncTests
        {
            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Inserts_entity_with_int32_key(IDialect dialect)
            {
                using (var instance = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var database = instance.Item;
                    var entities = new[]
                        {
                            new KeyInt32 { Name = "Some Name" },
                            new KeyInt32 { Name = "Some Name2" }
                        };

                    // Act
                    await database.InsertRangeAsync(entities);

                    // Assert
                    database.Count<KeyInt32>().Should().Be(2);

                    // Cleanup
                    database.DeleteAll<KeyInt32>();
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Inserts_entity_with_int64_key(IDialect dialect)
            {
                using (var instance = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var database = instance.Item;
                    var entities = new[]
                        {
                            new KeyInt64 { Name = "Some Name" },
                            new KeyInt64 { Name = "Some Name2" }
                        };

                    // Act
                    await database.InsertRangeAsync(entities);

                    // Assert
                    database.Count<KeyInt64>().Should().Be(2);

                    // Cleanup
                    database.DeleteAll<KeyInt64>();
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Inserts_entities_with_composite_keys(IDialect dialect)
            {
                using (var instance = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var database = instance.Item;
                    var entities = new[]
                        {
                            new CompositeKeys { Key1 = 2, Key2 = 3, Name = "Some Name1" },
                            new CompositeKeys { Key1 = 3, Key2 = 3, Name = "Some Name2" }
                        };

                    // Act
                    await database.InsertRangeAsync(entities);

                    // Assert
                    database.Count<CompositeKeys>().Should().Be(2);

                    // Cleanup
                    database.DeleteAll<CompositeKeys>();
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Inserts_entities_with_string_key(IDialect dialect)
            {
                using (var instance = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var database = instance.Item;
                    var entities = new[]
                        {
                            new KeyString { Name = "Some Name", Age = 10 },
                            new KeyString { Name = "Some Name2", Age = 11 }
                        };

                    // Act
                    await database.InsertRangeAsync(entities);

                    // Assert
                    database.Count<KeyString>().Should().Be(2);

                    // Cleanup
                    database.DeleteAll<KeyString>();
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Inserts_entities_with_guid_key(IDialect dialect)
            {
                using (var instance = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var database = instance.Item;
                    var entities = new[]
                        {
                            new KeyGuid { Id = Guid.NewGuid(), Name = "Some Name" },
                            new KeyGuid { Id = Guid.NewGuid(), Name = "Some Name2" }
                        };

                    // Act
                    await database.InsertRangeAsync(entities);

                    // Assert
                    database.Count<KeyGuid>().Should().Be(2);

                    // Cleanup
                    database.DeleteAll<KeyGuid>();
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Uses_key_attribute_to_determine_key(IDialect dialect)
            {
                using (var instance = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var database = instance.Item;
                    var entities = new[]
                        {
                            new KeyAlias { Name = "Some Name" },
                            new KeyAlias { Name = "Some Name2" }
                        };

                    // Act
                    await database.InsertRangeAsync(entities);

                    // Assert
                    database.Count<KeyAlias>().Should().Be(2);

                    // Cleanup
                    database.DeleteAll<KeyAlias>();
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Inserts_into_other_schemas(IDialect dialect)
            {
                using (var instance = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var database = instance.Item;
                    var entities = new[]
                        {
                            new SchemaOther { Name = "Some Name" },
                            new SchemaOther { Name = "Some Name2" }
                        };

                    // Act
                    await database.InsertRangeAsync(entities);

                    // Assert
                    database.Count<SchemaOther>().Should().Be(2);

                    // Cleanup
                    database.DeleteAll<SchemaOther>();
                }
            }
        }

        public abstract class InsertRangeAndSetKeyAsync
            : DefaultDatabaseConnectionCrudAsyncTests
        {
            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Throws_exception_when_entity_has_no_key(IDialect dialect)
            {
                using (var instance = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var database = instance.Item;
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
                using (var instance = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var database = instance.Item;
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
                using (var instance = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var database = instance.Item;
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
                using (var instance = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var database = instance.Item;
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
                using (var instance = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var database = instance.Item;
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

                    // Cleanup
                    database.DeleteAll<KeyInt32>();
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Inserts_entity_with_int64_primary_key(IDialect dialect)
            {
                using (var instance = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var database = instance.Item;
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

                    // Cleanup
                    database.DeleteAll<KeyInt64>();
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Uses_key_attribute_to_determine_key(IDialect dialect)
            {
                using (var instance = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var database = instance.Item;
                    var entities = new[]
                        {
                            new KeyExplicit { Name = "Some Name" }
                        };

                    // Act
                    await database.InsertRangeAsync<KeyExplicit, int>(entities, (e, k) => { e.Key = k; });

                    // Assert
                    entities[0].Key.Should().BeGreaterThan(0);

                    // Cleanup
                    database.DeleteAll<KeyAlias>();
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Inserts_into_other_schemas(IDialect dialect)
            {
                using (var instance = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var database = instance.Item;
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

                    // Cleanup
                    database.DeleteAll<SchemaOther>();
                }
            }
        }

        public abstract class UpdateAsync
            : DefaultDatabaseConnectionCrudAsyncTests
        {
            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Updates_the_entity(IDialect dialect)
            {
                using (var instance = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var database = instance.Item;
                    var id = database.Insert<int>(new User { Name = "Some name", Age = 10 });

                    // Act
                    var entity = database.Find<User>(id);
                    entity.Name = "Other name";
                    await database.UpdateAsync(entity);

                    // Assert
                    var updatedEntity = database.Find<User>(id);
                    updatedEntity.Name.Should().Be("Other name");

                    // Cleanup
                    database.Delete<User>(id);
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Ignores_columns_which_are_not_mapped(IDialect dialect)
            {
                using (var instance = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var database = instance.Item;
                    var entity = new PropertyNotMapped { Firstname = "Bobby", LastName = "DropTables", Age = 10 };
                    entity.Id = database.Insert<int>(entity);

                    // Act
                    entity.LastName = "Other name";
                    await database.UpdateAsync(entity);

                    // Assert
                    var updatedEntity = database.Find<PropertyNotMapped>(entity.Id);
                    updatedEntity.LastName.Should().Be("Other name");

                    // Cleanup
                    database.DeleteAll<PropertyNotMapped>();
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Updates_entities_with_composite_keys(IDialect dialect)
            {
                using (var instance = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var database = instance.Item;
                    var entity = new CompositeKeys { Key1 = 5, Key2 = 20, Name = "Some name" };
                    database.Insert(entity);

                    // Act
                    entity.Name = "Other name";
                    await database.UpdateAsync(entity);

                    // Assert
                    var id = new { Key1 = 5, Key2 = 20 };
                    var updatedEntity = database.Find<CompositeKeys>(id);

                    updatedEntity.Name.Should().Be("Other name");

                    // Cleanup
                    database.DeleteAll<CompositeKeys>();
                }
            }
        }

        public abstract class UpdateRangeAsync
            : DefaultDatabaseConnectionCrudAsyncTests
        {
            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Updates_the_entity(IDialect dialect)
            {
                using (var instance = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var database = instance.Item;
                    database.InsertRange(
                        new[]
                            {
                                new User { Name = "Some name1", Age = 10 },
                                new User { Name = "Some name2", Age = 10 },
                                new User { Name = "Some name2", Age = 11 }
                            });

                    // Act
                    var entities = database.GetRange<User>("WHERE Age = 10").ToList();
                    foreach (var entity in entities)
                    {
                        entity.Name = "Other name";
                    }

                    var result = await database.UpdateRangeAsync(entities);

                    // Assert
                    result.NumRowsAffected.Should().Be(2);

                    var updatedEntities = database.GetRange<User>("WHERE Name = 'Other name'");
                    updatedEntities.Count().Should().Be(2);

                    // Cleanup
                    database.DeleteAll<User>();
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Updates_entities_with_composite_keys(IDialect dialect)
            {
                using (var instance = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var database = instance.Item;
                    database.InsertRange(
                        new[]
                            {
                                new CompositeKeys { Key1 = 5, Key2 = 20, Name = "Some name1" },
                                new CompositeKeys { Key1 = 6, Key2 = 21, Name = "Some name2" },
                                new CompositeKeys { Key1 = 7, Key2 = 22, Name = "Some other name" }
                            });

                    // Act
                    var entities = database.GetRange<CompositeKeys>("WHERE Name Like 'Some name%'").ToList();

                    foreach (var entity in entities)
                    {
                        entity.Name = "Other name";
                    }

                    var result = await database.UpdateRangeAsync(entities);

                    // Assert
                    result.NumRowsAffected.Should().Be(2);

                    var updatedEntities = database.GetRange<CompositeKeys>("WHERE Name = 'Other name'");
                    updatedEntities.Count().Should().Be(2);

                    // Cleanup
                    database.DeleteAll<CompositeKeys>();
                }
            }
        }

        public abstract class DeleteIdAsync
            : DefaultDatabaseConnectionCrudAsyncTests
        {
            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Deletes_the_entity_with_the_specified_id(IDialect dialect)
            {
                using (var instance = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var database = instance.Item;
                    var id = database.Insert<int>(new User { Name = "Some name", Age = 10 });

                    // Act
                    await database.DeleteAsync<User>(id);

                    // Assert
                    database.Find<User>(id).Should().BeNull();
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Deletes_entity_with_string_key(IDialect dialect)
            {
                using (var instance = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var database = instance.Item;
                    database.Insert(new KeyString { Name = "Some Name", Age = 10 });

                    // Act
                    await database.DeleteAsync<KeyString>("Some Name");
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Deletes_entity_with_guid_key(IDialect dialect)
            {
                using (var instance = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var database = instance.Item;
                    var id = Guid.NewGuid();
                    database.Insert(new KeyGuid { Id = id, Name = "Some Name" });

                    // Act
                    await database.DeleteAsync<KeyGuid>(id);
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Deletes_entity_with_composite_keys(IDialect dialect)
            {
                using (var instance = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var database = instance.Item;
                    var id = new { Key1 = 5, Key2 = 20 };
                    var entity = new CompositeKeys { Key1 = 5, Key2 = 20, Name = "Some Name" };
                    database.Insert(entity);

                    // Act
                    await database.DeleteAsync<CompositeKeys>(id);
                }
            }
        }

        public abstract class DeleteEntityAsync
            : DefaultDatabaseConnectionCrudAsyncTests
        {
            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Deletes_entity_with_matching_key(IDialect dialect)
            {
                using (var instance = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var database = instance.Item;
                    var id = database.Insert<int>(new User { Name = "Some name", Age = 10 });

                    // Act
                    var entity = database.Find<User>(id);
                    await database.DeleteAsync(entity);

                    // Assert
                    database.Find<User>(id).Should().BeNull();
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Deletes_entity_with_composite_keys(IDialect dialect)
            {
                using (var instance = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var database = instance.Item;
                    var id = new { Key1 = 5, Key2 = 20 };
                    var entity = new CompositeKeys { Key1 = 5, Key2 = 20, Name = "Some Name" };
                    database.Insert(entity);

                    // Act
                    await database.DeleteAsync(entity);

                    // Assert
                    database.Find<CompositeKeys>(id).Should().BeNull();
                }
            }
        }

        public abstract class DeleteRangeAsync
            : DefaultDatabaseConnectionCrudAsyncTests
        {
            [Theory]
            [InlineData(null)]
            [InlineData("")]
            [InlineData(" ")]
            [InlineData("HAVING Age = 10")]
            [InlineData("WHERE")]
            public void Throws_exception_if_conditions_does_not_contain_where_clause(IDialect dialect, string conditions)
            {
                using (var instance = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var database = instance.Item;
                    // Act
                    Func<Task> act = async () => await database.DeleteRangeAsync<User>(conditions);

                    // Assert
                    act.ShouldThrow<ArgumentException>();
                }
            }

            [Theory]
            [InlineData("Where Age = 10")]
            [InlineData("where Age = 10")]
            [InlineData("WHERE Age = 10")]
            public void Allows_any_capitalization_of_where_clause(IDialect dialect, string conditions)
            {
                using (var instance = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var database = instance.Item;
                    // Act
                    Func<Task> act = async () => await database.DeleteRangeAsync<User>(conditions);

                    // Assert
                    act.ShouldNotThrow();
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Deletes_all_matching_entities(IDialect dialect)
            {
                using (var instance = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var database = instance.Item;
                    // Arrange
                    database.Insert(new User { Name = "Some Name 1", Age = 10 });
                    database.Insert(new User { Name = "Some Name 2", Age = 10 });
                    database.Insert(new User { Name = "Some Name 3", Age = 10 });
                    database.Insert(new User { Name = "Some Name 4", Age = 11 });

                    // Act
                    var result = await database.DeleteRangeAsync<User>("WHERE Age = @Age", new { Age = 10 });

                    // Assert
                    result.NumRowsAffected.Should().Be(3);
                    database.Count<User>().Should().Be(1);

                    // Cleanup
                    database.DeleteAll<User>();
                }
            }
        }

        public abstract class DeleteRangeAsyncWhereObject
            : DefaultDatabaseConnectionCrudAsyncTests
        {
            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Throws_exception_if_conditions_is_null(IDialect dialect)
            {
                using (var instance = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var database = instance.Item;
                    // Act
                    Func<Task> act = async () => await database.DeleteRangeAsync<User>((object)null);

                    // Assert
                    act.ShouldThrow<ArgumentNullException>();
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Throws_exception_if_conditions_is_empty(IDialect dialect)
            {
                using (var instance = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var database = instance.Item;
                    // Act
                    Func<Task> act = async () => await database.DeleteRangeAsync<User>(new { });

                    // Assert
                    act.ShouldThrow<ArgumentException>();
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Deletes_all_matching_entities(IDialect dialect)
            {
                using (var instance = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var database = instance.Item;
                    // Arrange
                    database.Insert(new User { Name = "Some Name 1", Age = 10 });
                    database.Insert(new User { Name = "Some Name 2", Age = 10 });
                    database.Insert(new User { Name = "Some Name 3", Age = 10 });
                    database.Insert(new User { Name = "Some Name 4", Age = 11 });

                    // Act
                    var result = await database.DeleteRangeAsync<User>(new { Age = 10 });

                    // Assert
                    result.NumRowsAffected.Should().Be(3);
                    database.Count<User>().Should().Be(1);

                    // Cleanup
                    database.DeleteAll<User>();
                }
            }
        }

        public abstract class DeleteAllAsync
            : DefaultDatabaseConnectionCrudAsyncTests
        {
            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Deletes_all_entities(IDialect dialect)
            {
                // Arrange
                using (var instance = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var database = instance.Item;
                    database.Insert(new User { Name = "Some Name 1", Age = 10 });
                    database.Insert(new User { Name = "Some Name 2", Age = 10 });
                    database.Insert(new User { Name = "Some Name 3", Age = 10 });
                    database.Insert(new User { Name = "Some Name 4", Age = 11 });

                    // Act
                    var result = await database.DeleteAllAsync<User>();

                    // Assert
                    result.NumRowsAffected.Should().Be(4);
                    database.Count<User>().Should().Be(0);
                }
            }
        }
    }
}