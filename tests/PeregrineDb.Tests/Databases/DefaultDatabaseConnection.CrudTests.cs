namespace PeregrineDb.Tests.Databases
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using FluentAssertions;
    using Pagination;
    using PeregrineDb.Dialects;
    using PeregrineDb.Schema;
    using PeregrineDb.SqlCommands;
    using PeregrineDb.Tests.ExampleEntities;
    using PeregrineDb.Tests.Utils;
    using Xunit;

    public abstract class DefaultDatabaseConnectionCrudTests
    {
        public static IEnumerable<object[]> TestDialects => new[]
            {
                new[] { Dialect.SqlServer2012 },
                new[] { Dialect.PostgreSql }
            };

        public static IEnumerable<object[]> TestDialectsWithData(string data) => new[]
            {
                new object[] { Dialect.SqlServer2012, data },
                new object[] { Dialect.PostgreSql, data }
            };

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
                    
                    database.Insert(new User { Name = "Some Name 1", Age = 10 });
                    database.Insert(new User { Name = "Some Name 2", Age = 10 });
                    database.Insert(new User { Name = "Some Name 3", Age = 10 });
                    database.Insert(new User { Name = "Some Name 4", Age = 11 });

                    // Act
                    var result = database.Count<User>();

                    // Assert
                    result.Should().Be(4);

                    // Cleanup
                    database.DeleteAll<User>();
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Counts_entities_matching_conditions(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    
                    database.Insert(new User { Name = "Some Name 1", Age = 10 });
                    database.Insert(new User { Name = "Some Name 2", Age = 10 });
                    database.Insert(new User { Name = "Some Name 3", Age = 10 });
                    database.Insert(new User { Name = "Some Name 4", Age = 11 });

                    // Act
                    var result = database.Count<User>($"WHERE Age < {11}");

                    // Assert
                    result.Should().Be(3);

                    // Cleanup
                    database.DeleteAll<User>();
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

                    // Cleanup
                    database.DeleteAll<SchemaOther>();
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
                    // Arrange
                    
                    // Act
                    Assert.Throws<ArgumentNullException>(() => database.Count<User>((object)null));
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Counts_all_entities_when_conditions_is_empty(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    
                    database.Insert(new User { Name = "Some Name 1", Age = 10 });
                    database.Insert(new User { Name = "Some Name 2", Age = 10 });
                    database.Insert(new User { Name = "Some Name 3", Age = 10 });
                    database.Insert(new User { Name = "Some Name 4", Age = 11 });

                    // Act
                    var result = database.Count<User>(new { });

                    // Assert
                    result.Should().Be(4);

                    // Cleanup
                    database.DeleteAll<User>();
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Counts_entities_matching_conditions(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    
                    database.Insert(new User { Name = "Some Name 1", Age = 10 });
                    database.Insert(new User { Name = "Some Name 2", Age = 10 });
                    database.Insert(new User { Name = "Some Name 3", Age = 10 });
                    database.Insert(new User { Name = "Some Name 4", Age = 11 });

                    // Act
                    var result = database.Count<User>(new { Age = 10 });

                    // Assert
                    result.Should().Be(3);

                    // Cleanup
                    database.DeleteAll<User>();
                }
            }
        }

        public class Find
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
                    Assert.Throws<InvalidPrimaryKeyException>(() => database.Find<NoKey>("Some Name"));
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Returns_null_when_entity_is_not_found(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    
                    // Act
                    var entity = database.Find<KeyInt32>(12);

                    // Assert
                    entity.Should().Be(null);
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
                    var entity = database.Find<KeyInt32>(id);

                    // Assert
                    entity.Name.Should().Be("Some Name");

                    // Cleanup
                    database.Delete<KeyInt32>(id);
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
                    var user = database.Find<KeyInt64>(id);

                    // Assert
                    user.Name.Should().Be("Some Name");

                    // Cleanup
                    database.Delete<KeyInt64>(id);
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
                    var entity = database.Find<KeyString>("Some Name");

                    // Assert
                    entity.Age.Should().Be(42);

                    // Cleanup
                    database.Delete(entity);
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
                    var entity = database.Find<KeyGuid>(id);

                    // Assert
                    entity.Name.Should().Be("Some Name");

                    // Cleanup
                    database.Delete(entity);
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
                    var entity = database.Find<CompositeKeys>(id);

                    // Assert
                    entity.Name.Should().Be("Some Name");

                    // Cleanup
                    database.DeleteAll<CompositeKeys>();
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
                    var entity = database.Find<SchemaOther>(id);

                    // Assert
                    entity.Name.Should().Be("Some Name");

                    // Cleanup
                    database.Delete<SchemaOther>(id);
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Finds_entities_with_enum_property(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    
                    var id = database.Insert<int>(new PropertyEnum { FavoriteColor = Color.Green });

                    // Act
                    var entity = database.Find<PropertyEnum>(id);

                    // Assert
                    entity.FavoriteColor.Should().Be(Color.Green);

                    // Cleanup
                    database.Delete<PropertyEnum>(id);
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Finds_entities_with_all_possible_types(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    
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
                                DateTimeOffsetProperty =
                                    new DateTimeOffset(new DateTime(2016, 12, 31), new TimeSpan(0, 1, 0, 0)),
                                NullableDateTimeOffsetProperty =
                                    new DateTimeOffset(new DateTime(2016, 12, 31), new TimeSpan(0, 1, 0, 0)),
                                ByteArrayProperty = new byte[] { 1, 2, 3 }
                            }
                    );

                    // Act
                    var entity = database.Find<PropertyAllPossibleTypes>(id);

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
                    entity.ByteArrayProperty.ShouldAllBeEquivalentTo(new byte[]
                        {
                            1, 2, 3
                        }, o => o.WithStrictOrdering());

                    // Cleanup
                    database.Delete<PropertyAllPossibleTypes>(id);
                }
            }



            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Ignores_columns_which_are_not_mapped(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    
                    var id = database.Insert<int>(new PropertyNotMapped { Firstname = "Bobby", LastName = "DropTables", Age = 10 });

                    // Act
                    var entity = database.Find<PropertyNotMapped>(id);

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
                    // Arrange
                    
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

                    // Cleanup
                    database.Delete<KeyInt32>(id);
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
                    var user = database.Get<KeyInt64>(id);

                    // Assert
                    user.Name.Should().Be("Some Name");

                    // Cleanup
                    database.Delete<KeyInt64>(id);
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

                    // Cleanup
                    database.Delete(entity);
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

                    // Cleanup
                    database.Delete(entity);
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

                    // Cleanup
                    database.DeleteAll<CompositeKeys>();
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

                    // Cleanup
                    database.Delete<SchemaOther>(id);
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Ignores_columns_which_are_not_mapped(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    
                    var id = database.Insert<int>(new PropertyNotMapped { Firstname = "Bobby", LastName = "DropTables", Age = 10 });

                    // Act
                    var entity = database.Get<PropertyNotMapped>(id);

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

        public class GetFirstOrDefault
            : DefaultDatabaseConnectionCrudTests
        {

            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Gets_first_matching_result(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    
                    database.Insert(new User { Name = "Some Name 1", Age = 10 });
                    database.Insert(new User { Name = "Some Name 2", Age = 10 });
                    database.Insert(new User { Name = "Some Name 3", Age = 10 });
                    database.Insert(new User { Name = "Some Name 4", Age = 11 });

                    // Act
                    var user = database.GetFirstOrDefault<User>(
                        $"WHERE Name LIKE CONCAT({"Some Name"}, '%') and Age = {10}",
                        "Name DESC");

                    // Assert
                    user.ShouldBeEquivalentTo(new User { Name = "Some Name 3", Age = 10 }, o => o.Excluding(e => e.Id));

                    // Cleanup
                    database.DeleteAll<User>();
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Returns_default_when_no_entity_matches(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    
                    // Act
                    var user = database.GetFirstOrDefault<User>(
                        $"WHERE Name LIKE CONCAT({"Some Name"}, '%') and Age = {10}",
                        "Name DESC");

                    // Assert
                    user.Should().BeNull();
                }
            }
        }

        public class GetFirstOrDefaultWhereObject
            : DefaultDatabaseConnectionCrudTests
        {

            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Gets_first_matching_result(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    
                    database.Insert(new User { Name = "Some Name 1", Age = 10 });
                    database.Insert(new User { Name = "Some Name 2", Age = 10 });
                    database.Insert(new User { Name = "Some Name 3", Age = 10 });
                    database.Insert(new User { Name = "Some Name 4", Age = 11 });

                    // Act
                    var user = database.GetFirstOrDefault<User>(new { Age = 10 }, "Name DESC");

                    // Assert
                    user.ShouldBeEquivalentTo(new User { Name = "Some Name 3", Age = 10 }, o => o.Excluding(e => e.Id));

                    // Cleanup
                    database.DeleteAll<User>();
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Returns_default_when_no_entity_matches(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    
                    // Act
                    var user = database.GetFirstOrDefault<User>(new { Age = 10 }, "Name DESC");

                    // Assert
                    user.Should().BeNull();
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
                    
                    database.Insert(new User { Name = "Some Name 1", Age = 10 });
                    database.Insert(new User { Name = "Some Name 2", Age = 10 });
                    database.Insert(new User { Name = "Some Name 3", Age = 10 });
                    database.Insert(new User { Name = "Some Name 4", Age = 11 });

                    // Act
                    var user = database.GetFirst<User>(
                        $"WHERE Name LIKE CONCAT({"Some Name"}, '%') and Age = {10}",
                        "Name DESC");

                    // Assert
                    user.ShouldBeEquivalentTo(new User { Name = "Some Name 3", Age = 10 }, o => o.Excluding(e => e.Id));

                    // Cleanup
                    database.DeleteAll<User>();
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Throws_exception_when_no_entity_matches(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Act
                    Action act = () => database.GetFirst<User>($"WHERE Name LIKE CONCAT({"Some Name"}, '%') and Age = {10}", "Name DESC");

                    // Assert
                    act.ShouldThrow<InvalidOperationException>();
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
                    
                    database.Insert(new User { Name = "Some Name 1", Age = 10 });
                    database.Insert(new User { Name = "Some Name 2", Age = 10 });
                    database.Insert(new User { Name = "Some Name 3", Age = 10 });
                    database.Insert(new User { Name = "Some Name 4", Age = 11 });

                    // Act
                    var user = database.GetFirst<User>(new { Age = 10 }, "Name DESC");

                    // Assert
                    user.ShouldBeEquivalentTo(new User { Name = "Some Name 3", Age = 10 }, o => o.Excluding(e => e.Id));

                    // Cleanup
                    database.DeleteAll<User>();
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Throws_exception_when_no_entity_matches(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Act
                    Action act = () => database.GetFirst<User>(new { Age = 10 }, "Name DESC");

                    // Assert
                    act.ShouldThrow<InvalidOperationException>();
                }
            }
        }

        public class GetSingleOrDefault
            : DefaultDatabaseConnectionCrudTests
        {

            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Gets_only_matching_result(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    database.Insert(new User { Name = "Some Name 1", Age = 10 });
                    database.Insert(new User { Name = "Some Name 2", Age = 11 });

                    // Act
                    var user = database.GetSingleOrDefault<User>($"WHERE Name LIKE CONCAT({"Some Name"}, '%') and Age = {10}");

                    // Assert
                    user.ShouldBeEquivalentTo(new User { Name = "Some Name 1", Age = 10 }, o => o.Excluding(e => e.Id));

                    // Cleanup
                    database.DeleteAll<User>();
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Returns_default_when_no_entity_matches(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Act
                    var user = database.GetSingleOrDefault<User>($"WHERE Name LIKE CONCAT({"Some Name"}, '%') and Age = {10}");

                    // Assert
                    user.Should().BeNull();
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Throws_exception_when_multiple_entities_match(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    database.Insert(new User { Name = "Some Name 1", Age = 10 });
                    database.Insert(new User { Name = "Some Name 2", Age = 10 });

                    // Act
                    Action act = () => database.GetSingleOrDefault<User>($"WHERE Age = {10}");

                    // Assert
                    act.ShouldThrow<InvalidOperationException>();

                    // Cleanup
                    database.DeleteAll<User>();
                }
            }
        }

        public class GetSingleOrDefaultWhereObject
            : DefaultDatabaseConnectionCrudTests
        {

            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Gets_first_matching_result(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    database.Insert(new User { Name = "Some Name 1", Age = 10 });
                    database.Insert(new User { Name = "Some Name 2", Age = 11 });

                    // Act
                    var user = database.GetSingleOrDefault<User>(new { Age = 10 });

                    // Assert
                    user.ShouldBeEquivalentTo(new User { Name = "Some Name 1", Age = 10 }, o => o.Excluding(e => e.Id));

                    // Cleanup
                    database.DeleteAll<User>();
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Returns_default_when_no_entity_matches(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Act
                    var user = database.GetSingleOrDefault<User>(new { Age = 10 });

                    // Assert
                    user.Should().BeNull();
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Throws_exception_when_multiple_entities_match(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    database.Insert(new User { Name = "Some Name 1", Age = 10 });
                    database.Insert(new User { Name = "Some Name 2", Age = 10 });

                    // Act
                    Action act = () => database.GetSingleOrDefault<User>(new { Age = 10 });

                    // Assert
                    act.ShouldThrow<InvalidOperationException>();

                    // Cleanup
                    database.DeleteAll<User>();
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
                    database.Insert(new User { Name = "Some Name 1", Age = 10 });
                    database.Insert(new User { Name = "Some Name 2", Age = 11 });

                    // Act
                    var user = database.GetSingle<User>($"WHERE Name LIKE CONCAT({"Some Name"}, '%') and Age = {10}");

                    // Assert
                    user.ShouldBeEquivalentTo(new User { Name = "Some Name 1", Age = 10 }, o => o.Excluding(e => e.Id));

                    // Cleanup
                    database.DeleteAll<User>();
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Throws_exception_when_no_entity_matches(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Act
                    Action act = () => database.GetSingle<User>($"WHERE Name LIKE CONCAT({"Some Name"}, '%') and Age = {10}");

                    // Assert
                    act.ShouldThrow<InvalidOperationException>();
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Throws_exception_when_multiple_entities_match(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    database.Insert(new User { Name = "Some Name 1", Age = 10 });
                    database.Insert(new User { Name = "Some Name 2", Age = 10 });

                    // Act
                    Action act = () => database.GetSingle<User>($"WHERE Age = {10}");

                    // Assert
                    act.ShouldThrow<InvalidOperationException>();

                    // Cleanup
                    database.DeleteAll<User>();
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
                    database.Insert(new User { Name = "Some Name 1", Age = 10 });
                    database.Insert(new User { Name = "Some Name 2", Age = 11 });

                    // Act
                    var user = database.GetSingle<User>(new { Age = 10 });

                    // Assert
                    user.ShouldBeEquivalentTo(new User { Name = "Some Name 1", Age = 10 }, o => o.Excluding(e => e.Id));

                    // Cleanup
                    database.DeleteAll<User>();
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Throws_exception_when_no_entity_matches(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Act
                    Action act = () => database.GetSingle<User>(new { Age = 10 });

                    // Assert
                    act.ShouldThrow<InvalidOperationException>();
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Throws_exception_when_multiple_entities_match(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    database.Insert(new User { Name = "Some Name 1", Age = 10 });
                    database.Insert(new User { Name = "Some Name 2", Age = 10 });

                    // Act
                    Action act = () => database.GetSingle<User>(new { Age = 10 });

                    // Assert
                    act.ShouldThrow<InvalidOperationException>();

                    // Cleanup
                    database.DeleteAll<User>();
                }
            }
        }

        public class GetRange
            : DefaultDatabaseConnectionCrudTests
        {

            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Filters_result_by_conditions(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    database.Insert(new User { Name = "Some Name 1", Age = 10 });
                    database.Insert(new User { Name = "Some Name 2", Age = 10 });
                    database.Insert(new User { Name = "Some Name 3", Age = 10 });
                    database.Insert(new User { Name = "Some Name 4", Age = 11 });

                    // Act
                    var users = database.GetRange<User>($"WHERE Name LIKE CONCAT({"Some Name"}, '%') and Age = {10}");

                    // Assert
                    users.Count().Should().Be(3);

                    // Cleanup
                    database.DeleteAll<User>();
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Returns_everything_when_conditions_is_null(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    database.Insert(new User { Name = "Some Name 1", Age = 10 });
                    database.Insert(new User { Name = "Some Name 2", Age = 10 });
                    database.Insert(new User { Name = "Some Name 3", Age = 10 });
                    database.Insert(new User { Name = "Some Name 4", Age = 11 });

                    // Act
                    var users = database.GetRange<User>(null);

                    // Assert
                    users.Count().Should().Be(4);

                    // Cleanup
                    database.DeleteAll<User>();
                }
            }
        }

        public class GetRangeWhereObject
            : DefaultDatabaseConnectionCrudTests
        {

            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Throws_exception_when_conditions_is_null(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Act
                    Assert.Throws<ArgumentNullException>(() => database.GetRange<User>((object)null));
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Returns_all_when_conditions_is_empty(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    database.Insert(new User { Name = "Some Name 1", Age = 10 });
                    database.Insert(new User { Name = "Some Name 2", Age = 10 });
                    database.Insert(new User { Name = "Some Name 3", Age = 10 });
                    database.Insert(new User { Name = "Some Name 4", Age = 11 });

                    // Act
                    var users = database.GetRange<User>(new { });

                    // Assert
                    users.Count().Should().Be(4);

                    // Cleanup
                    database.DeleteAll<User>();
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Filters_result_by_conditions(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    
                    database.Insert(new User { Name = "Some Name 1", Age = 10 });
                    database.Insert(new User { Name = "Some Name 2", Age = 10 });
                    database.Insert(new User { Name = "Some Name 3", Age = 10 });
                    database.Insert(new User { Name = "Some Name 4", Age = 11 });

                    // Act
                    var users = database.GetRange<User>(new { Age = 10 });

                    // Assert
                    users.Count().Should().Be(3);

                    // Cleanup
                    database.DeleteAll<User>();
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Matches_column_name_case_insensitively(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    
                    database.Insert(new User { Name = "Some Name 1", Age = 10 });
                    database.Insert(new User { Name = "Some Name 2", Age = 10 });
                    database.Insert(new User { Name = "Some Name 3", Age = 10 });
                    database.Insert(new User { Name = "Some Name 4", Age = 11 });

                    // Act
                    var users = database.GetRange<User>(new { age = 10 });

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
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    
                    // Act
                    Action act = () => database.GetRange<User>(new { Ages = 10 });

                    // Assert
                    act.ShouldThrow<InvalidConditionSchemaException>();
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public void When_value_is_not_null_does_not_find_nulls(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    
                    database.Insert(new PropertyNullable { Name = null });
                    database.Insert(new PropertyNullable { Name = "Some Name 3" });
                    database.Insert(new PropertyNullable { Name = null });

                    // Act
                    var users = database.GetRange<PropertyNullable>(new { Name = "Some Name 3" });

                    // Assert
                    users.Count().Should().Be(1);

                    // Cleanup
                    database.DeleteAll<PropertyNullable>();
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public void When_value_is_null_finds_nulls(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    
                    database.Insert(new PropertyNullable { Name = null });
                    database.Insert(new PropertyNullable { Name = "Some Name 3" });
                    database.Insert(new PropertyNullable { Name = null });

                    // Act
                    var users = database.GetRange<PropertyNullable>(new { Name = (string)null });

                    // Assert
                    users.Count().Should().Be(2);

                    // Cleanup
                    database.DeleteAll<PropertyNullable>();
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Filters_on_multiple_properties(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    
                    database.Insert(new User { Name = "Some Name 1", Age = 10 });
                    database.Insert(new User { Name = "Some Name 2", Age = 10 });
                    database.Insert(new User { Name = "Some Name 2", Age = 12 });

                    // Act
                    var users = database.GetRange<User>(new { Name = "Some Name 2", Age = 10 });

                    // Assert
                    users.Count().Should().Be(1);

                    // Cleanup
                    database.DeleteAll<User>();
                }
            }
        }

        public class GetPage
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
                    var users = database.GetPage<User>(pageBuilder, null, "Age");

                    // Assert
                    users.Items.Count().Should().Be(0);
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Filters_result_by_conditions(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    
                    database.Insert(new User { Name = "Some Name 1", Age = 10 });
                    database.Insert(new User { Name = "Some Name 2", Age = 10 });
                    database.Insert(new User { Name = "Some Name 3", Age = 10 });
                    database.Insert(new User { Name = "Some Name 4", Age = 11 });

                    // Act
                    var users = database.GetPage<User>(
                        new PageIndexPageBuilder(1, 10),
                        $"WHERE Name LIKE CONCAT({"Some Name"}, '%') and Age = {10}",
                        "Age");

                    // Assert
                    users.Items.Count().Should().Be(3);

                    // Cleanup
                    database.DeleteAll<User>();
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Gets_first_page(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    
                    database.Insert(new User { Name = "Some Name 1", Age = 10 });
                    database.Insert(new User { Name = "Some Name 2", Age = 10 });
                    database.Insert(new User { Name = "Some Name 3", Age = 10 });
                    database.Insert(new User { Name = "Some Name 4", Age = 11 });

                    // Act
                    var users = database.GetPage<User>(
                        new PageIndexPageBuilder(1, 2),
                        $"WHERE Name LIKE CONCAT({"Some Name"}, '%') and Age = {10}",
                        "Age DESC").Items;

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
            public void Gets_second_page(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    
                    database.Insert(new User { Name = "Some Name 1", Age = 10 });
                    database.Insert(new User { Name = "Some Name 2", Age = 10 });
                    database.Insert(new User { Name = "Some Name 3", Age = 10 });
                    database.Insert(new User { Name = "Some Name 4", Age = 11 });

                    // Act
                    var users = database.GetPage<User>(
                        new PageIndexPageBuilder(2, 2),
                        $"WHERE Name LIKE CONCAT({"Some Name"}, '%') and Age = {10}",
                        "Age DESC").Items;

                    // Assert
                    users.Count().Should().Be(1);
                    users[0].Name.Should().Be("Some Name 3");

                    // Cleanup
                    database.DeleteAll<User>();
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Returns_empty_set_past_last_page(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    
                    database.Insert(new User { Name = "Some Name 1", Age = 10 });
                    database.Insert(new User { Name = "Some Name 2", Age = 10 });
                    database.Insert(new User { Name = "Some Name 3", Age = 10 });
                    database.Insert(new User { Name = "Some Name 4", Age = 11 });

                    // Act
                    var users = database.GetPage<User>(
                        new PageIndexPageBuilder(3, 2),
                        $"WHERE Name LIKE CONCAT({"Some Name"}, '%') and Age = {10}",
                        "Age DESC").Items;

                    // Assert
                    users.Should().BeEmpty();

                    // Cleanup
                    database.DeleteAll<User>();
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Returns_page_from_everything_when_conditions_is_null(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    
                    database.Insert(new User { Name = "Some Name 1", Age = 10 });
                    database.Insert(new User { Name = "Some Name 2", Age = 10 });
                    database.Insert(new User { Name = "Some Name 3", Age = 10 });
                    database.Insert(new User { Name = "Some Name 4", Age = 11 });

                    // Act
                    var users = database.GetPage<User>(new PageIndexPageBuilder(2, 2), null, "Age DESC").Items;

                    // Assert
                    users.Count().Should().Be(2);

                    // Cleanup
                    database.DeleteAll<User>();
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
                    var users = database.GetPage<User>(pageBuilder, new { Age = 10 }, "Age");

                    // Assert
                    users.Items.Should().BeEmpty();
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Filters_result_by_conditions(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    
                    database.Insert(new User { Name = "Some Name 1", Age = 10 });
                    database.Insert(new User { Name = "Some Name 2", Age = 10 });
                    database.Insert(new User { Name = "Some Name 3", Age = 10 });
                    database.Insert(new User { Name = "Some Name 4", Age = 11 });

                    // Act
                    var pageBuilder = new PageIndexPageBuilder(1, 10);
                    var users = database.GetPage<User>(pageBuilder, new { Age = 10 }, "Age");

                    // Assert
                    users.Items.Count().Should().Be(3);

                    // Cleanup
                    database.DeleteAll<User>();
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Gets_first_page(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    
                    database.Insert(new User { Name = "Some Name 1", Age = 10 });
                    database.Insert(new User { Name = "Some Name 2", Age = 10 });
                    database.Insert(new User { Name = "Some Name 3", Age = 10 });
                    database.Insert(new User { Name = "Some Name 4", Age = 11 });

                    // Act
                    var pageBuilder = new PageIndexPageBuilder(1, 2);
                    var page = database.GetPage<User>(pageBuilder, new { Age = 10 }, "Age");
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
            public void Gets_second_page(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    
                    database.Insert(new User { Name = "Some Name 1", Age = 10 });
                    database.Insert(new User { Name = "Some Name 2", Age = 10 });
                    database.Insert(new User { Name = "Some Name 3", Age = 10 });
                    database.Insert(new User { Name = "Some Name 4", Age = 11 });

                    // Act
                    var pageBuilder = new PageIndexPageBuilder(2, 2);
                    var page = database.GetPage<User>(pageBuilder, new { Age = 10 }, "Age");
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
            public void Returns_empty_set_past_last_page(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    
                    database.Insert(new User { Name = "Some Name 1", Age = 10 });
                    database.Insert(new User { Name = "Some Name 2", Age = 10 });
                    database.Insert(new User { Name = "Some Name 3", Age = 10 });
                    database.Insert(new User { Name = "Some Name 4", Age = 11 });

                    // Act
                    var pageBuilder = new PageIndexPageBuilder(3, 2);
                    var page = database.GetPage<User>(pageBuilder, new { Age = 10 }, "Age");
                    var users = page.Items;

                    // Assert
                    users.Should().BeEmpty();

                    // Cleanup
                    database.DeleteAll<User>();
                }
            }
        }


        public class GetAll
            : DefaultDatabaseConnectionCrudTests
        {

            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Gets_all(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    
                    database.Insert(new User { Name = "Some Name 1", Age = 10 });
                    database.Insert(new User { Name = "Some Name 2", Age = 10 });
                    database.Insert(new User { Name = "Some Name 3", Age = 10 });
                    database.Insert(new User { Name = "Some Name 4", Age = 11 });

                    // Act
                    var users = database.GetAll<User>();

                    // Assert
                    users.Count().Should().Be(4);

                    // Cleanup
                    database.DeleteAll<User>();
                }
            }
        }

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

                    // Cleanup
                    database.DeleteAll<KeyInt32>();
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

                    // Cleanup
                    database.DeleteAll<KeyInt64>();
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

                    // Cleanup
                    database.DeleteAll<CompositeKeys>();
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

                    // Cleanup
                    database.DeleteAll<KeyString>();
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

                    // Cleanup
                    database.DeleteAll<KeyGuid>();
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

                    // Cleanup
                    database.DeleteAll<KeyAlias>();
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

                    // Cleanup
                    database.DeleteAll<SchemaOther>();
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Ignores_columns_which_are_not_mapped(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    
                    var entity = new PropertyNotMapped { Firstname = "Bobby", LastName = "DropTables", Age = 10 };

                    // Act
                    database.Insert(entity);

                    // Assert
                    database.Count<PropertyNotMapped>().Should().Be(1);

                    // Cleanup
                    database.DeleteAll<PropertyNotMapped>();
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
                    // Arrange
                    
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
                    // Arrange
                    
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
                    // Arrange
                    
                    // Act
                    var id = database.Insert<int>(new KeyInt32 { Name = "Some Name" });

                    // Assert
                    id.Should().BeGreaterThan(0);

                    // Cleanup
                    database.Delete<KeyInt32>(id);
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Inserts_entity_with_int64_primary_key(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    
                    // Act
                    var id = database.Insert<int>(new KeyInt64 { Name = "Some Name" });

                    // Assert
                    id.Should().BeGreaterThan(0);

                    // Cleanup
                    database.Delete<KeyInt64>(id);
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Uses_key_attribute_to_determine_key(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    
                    // Act
                    var id = database.Insert<int>(new KeyAlias { Name = "Some Name" });

                    // Assert
                    id.Should().BeGreaterThan(0);

                    // Cleanup
                    database.Delete<KeyAlias>(id);
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Inserts_into_other_schemas(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    
                    // Act
                    var id = database.Insert<int>(new SchemaOther { Name = "Some name" });

                    // Assert
                    id.Should().BeGreaterThan(0);

                    // Cleanup
                    database.Delete<SchemaOther>(id);
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

                    // Cleanup
                    database.DeleteAll<KeyInt32>();
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

                    // Cleanup
                    database.DeleteAll<KeyInt64>();
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

                    // Cleanup
                    database.DeleteAll<CompositeKeys>();
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

                    // Cleanup
                    database.DeleteAll<KeyString>();
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

                    // Cleanup
                    database.DeleteAll<KeyGuid>();
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

                    // Cleanup
                    database.DeleteAll<KeyAlias>();
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

                    // Cleanup
                    database.DeleteAll<SchemaOther>();
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

                    // Cleanup
                    database.DeleteAll<KeyInt32>();
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

                    // Cleanup
                    database.DeleteAll<KeyInt64>();
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

                    // Cleanup
                    database.DeleteAll<KeyAlias>();
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

                    // Cleanup
                    database.DeleteAll<SchemaOther>();
                }
            }
        }

        public class Update
            : DefaultDatabaseConnectionCrudTests
        {

            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Updates_the_entity(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    
                    var id = database.Insert<int>(new User { Name = "Some name", Age = 10 });

                    // Act
                    var entity = database.Find<User>(id);
                    entity.Name = "Other name";
                    database.Update(entity);

                    // Assert
                    var updatedEntity = database.Find<User>(id);
                    updatedEntity.Name.Should().Be("Other name");

                    // Cleanup
                    database.Delete<User>(id);
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Ignores_columns_which_are_not_mapped(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    
                    var entity = new PropertyNotMapped { Firstname = "Bobby", LastName = "DropTables", Age = 10 };
                    entity.Id = database.Insert<int>(entity);

                    // Act
                    entity.LastName = "Other name";
                    database.Update(entity);

                    // Assert
                    var updatedEntity = database.Find<PropertyNotMapped>(entity.Id);
                    updatedEntity.LastName.Should().Be("Other name");

                    // Cleanup
                    database.DeleteAll<PropertyNotMapped>();
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Updates_entities_with_composite_keys(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var entity = new CompositeKeys { Key1 = 5, Key2 = 20, Name = "Some name" };
                    database.Insert(entity);

                    // Act
                    entity.Name = "Other name";
                    database.Update(entity);

                    // Assert
                    var id = new { Key1 = 5, Key2 = 20 };
                    var updatedEntity = database.Find<CompositeKeys>(id);

                    updatedEntity.Name.Should().Be("Other name");

                    // Cleanup
                    database.DeleteAll<CompositeKeys>();
                }
            }
        }

        public class UpdateRange
            : DefaultDatabaseConnectionCrudTests
        {

            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Updates_the_entity(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    database.InsertRange(
                        new[]
                            {
                                new User { Name = "Some name1", Age = 10 },
                                new User { Name = "Some name2", Age = 10 },
                                new User { Name = "Some name2", Age = 11 }
                            });

                    // Act
                    var entities = database.GetRange<User>($"WHERE Age = 10").ToList();
                    foreach (var entity in entities)
                    {
                        entity.Name = "Other name";
                    }


                    var result = database.UpdateRange(entities);

                    // Assert
                    result.NumRowsAffected.Should().Be(2);

                    var updatedEntities = database.GetRange<User>($"WHERE Name = 'Other name'");
                    updatedEntities.Count().Should().Be(2);

                    // Cleanup
                    database.DeleteAll<User>();
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Updates_entities_with_composite_keys(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    database.InsertRange(
                        new[]
                            {
                                new CompositeKeys { Key1 = 5, Key2 = 20, Name = "Some name1" },
                                new CompositeKeys { Key1 = 6, Key2 = 21, Name = "Some name2" },
                                new CompositeKeys { Key1 = 7, Key2 = 22, Name = "Some other name" }
                            });

                    // Act
                    var entities = database.GetRange<CompositeKeys>($"WHERE Name Like 'Some name%'").ToList();

                    foreach (var entity in entities)
                    {
                        entity.Name = "Other name";
                    }

                    var result = database.UpdateRange(entities);

                    // Assert
                    result.NumRowsAffected.Should().Be(2);

                    var updatedEntities = database.GetRange<CompositeKeys>($"WHERE Name = 'Other name'");
                    updatedEntities.Count().Should().Be(2);

                    // Cleanup
                    database.DeleteAll<CompositeKeys>();
                }
            }

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
                        
                        var id = database.Insert<int>(new User { Name = "Some name", Age = 10 });

                        // Act
                        database.Delete<User>(id);

                        // Assert
                        database.Find<User>(id).Should().BeNull();
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
                        
                        var id = database.Insert<int>(new User { Name = "Some name", Age = 10 });

                        // Act
                        var entity = database.Find<User>(id);
                        database.Delete(entity);

                        // Assert
                        database.Find<User>(id).Should().BeNull();
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
                        // Arrange
                        
                        var actualCondition = conditions != null ? new SqlString(conditions) : null;

                        // Act / Assert
                        Assert.Throws<ArgumentException>(() => database.DeleteRange<User>(actualCondition));
                    }
                }

                [Theory]
                [MemberData(nameof(TestDialectsWithData), "Where Age = 10")]
                [MemberData(nameof(TestDialectsWithData), "where Age = 10")]
                [MemberData(nameof(TestDialectsWithData), "WHERE Age = 10")]
                public void Allows_any_capitalization_of_where_clause(IDialect dialect, string conditions)
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                    {
                        // Arrange
                        
                        // Act
                        Action act = () => database.DeleteRange<User>(new SqlString(conditions));

                        // Assert
                        act.ShouldNotThrow();
                    }
                }

                [Theory]
                [MemberData(nameof(TestDialects))]
                public void Deletes_all_matching_entities(IDialect dialect)
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                    {
                        // Arrange
                        
                        database.Insert(new User { Name = "Some Name 1", Age = 10 });
                        database.Insert(new User { Name = "Some Name 2", Age = 10 });
                        database.Insert(new User { Name = "Some Name 3", Age = 10 });
                        database.Insert(new User { Name = "Some Name 4", Age = 11 });

                        // Act
                        var result = database.DeleteRange<User>($"WHERE Age = {10}");

                        // Assert
                        result.NumRowsAffected.Should().Be(3);
                        database.Count<User>().Should().Be(1);

                        // Cleanup
                        database.DeleteAll<User>();
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
                        // Arrange
                        
                        // Act / Assert
                        Assert.Throws<ArgumentNullException>(() => database.DeleteRange<User>((object)null));
                    }
                }


                [Theory]
                [MemberData(nameof(TestDialects))]
                public void Throws_exception_if_conditions_is_empty(IDialect dialect)
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                    {
                        // Arrange
                        
                        // Act / Assert
                        Assert.Throws<ArgumentException>(() => database.DeleteRange<User>(new { }));
                    }
                }


                [Theory]
                [MemberData(nameof(TestDialects))]
                public void Deletes_all_matching_entities(IDialect dialect)
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                    {
                        // Arrange
                        
                        database.Insert(new User { Name = "Some Name 1", Age = 10 });
                        database.Insert(new User { Name = "Some Name 2", Age = 10 });
                        database.Insert(new User { Name = "Some Name 3", Age = 10 });
                        database.Insert(new User { Name = "Some Name 4", Age = 11 });

                        // Act
                        var result = database.DeleteRange<User>(new { Age = 10 });

                        // Assert
                        result.NumRowsAffected.Should().Be(3);
                        database.Count<User>().Should().Be(1);

                        // Cleanup
                        database.DeleteAll<User>();
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
                        
                        database.Insert(new User { Name = "Some Name 1", Age = 10 });
                        database.Insert(new User { Name = "Some Name 2", Age = 10 });
                        database.Insert(new User { Name = "Some Name 3", Age = 10 });
                        database.Insert(new User { Name = "Some Name 4", Age = 11 });

                        // Act
                        var result = database.DeleteAll<User>();

                        // Assert
                        result.NumRowsAffected.Should().Be(4);
                        database.Count<User>().Should().Be(0);
                    }
                }
            }
        }
    }
}