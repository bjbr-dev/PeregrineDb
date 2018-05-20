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
                    Action act = () => database.Find<NoKey>("Some Name");

                    // Assert
                    act.ShouldThrow<InvalidPrimaryKeyException>();
                }
            }

            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Returns_null_when_entity_is_not_found(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
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
                    var entity = database.Find<KeyInt64>(id);

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
                    var entity = database.Find<KeyString>("Some Name");

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
                    var entity = database.Find<KeyGuid>(id);

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
                    var entity = database.Find<CompositeKeys>(id);

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
                    var entity = database.Find<SchemaOther>(id);

                    // Assert
                    entity.Name.Should().Be("Some Name");
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
                    var entity = database.Find<PropertyNotMapped>(id);

                    // Assert
                    entity.FirstName.Should().Be("Bobby");
                    entity.LastName.Should().Be("DropTables");
                    entity.FullName.Should().Be("Bobby DropTables");
                    entity.Age.Should().Be(0);
                }
            }
        }

        public class FindFirst
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
                    var entity = database.FindFirst<Dog>(
                        $"WHERE Name LIKE CONCAT({"Some Name"}, '%') and Age = {10}",
                        "Name DESC");

                    // Assert
                    entity.ShouldBeEquivalentTo(new Dog { Name = "Some Name 3", Age = 10 }, o => o.Excluding(e => e.Id));
                }
            }

            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Returns_default_when_no_entity_matches(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Act
                    var entity = database.FindFirst<Dog>(
                        $"WHERE Name LIKE CONCAT({"Some Name"}, '%') and Age = {10}",
                        "Name DESC");

                    // Assert
                    entity.Should().BeNull();
                }
            }
        }

        public class FindFirstWhereObject
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
                    var entity = database.FindFirst<Dog>(new { Age = 10 }, "Name DESC");

                    // Assert
                    entity.ShouldBeEquivalentTo(new Dog { Name = "Some Name 3", Age = 10 }, o => o.Excluding(e => e.Id));
                }
            }

            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Returns_default_when_no_entity_matches(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Act
                    var entity = database.FindFirst<Dog>(new { Age = 10 }, "Name DESC");

                    // Assert
                    entity.Should().BeNull();
                }
            }
        }

        public class FindSingle
            : DefaultDatabaseConnectionCrudTests
        {
            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Gets_only_matching_result(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    database.Insert(new Dog { Name = "Some Name 1", Age = 10 });
                    database.Insert(new Dog { Name = "Some Name 2", Age = 11 });

                    // Act
                    var entity = database.FindSingle<Dog>($"WHERE Name LIKE CONCAT({"Some Name"}, '%') and Age = {10}");

                    // Assert
                    entity.ShouldBeEquivalentTo(new Dog { Name = "Some Name 1", Age = 10 }, o => o.Excluding(e => e.Id));
                }
            }

            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Returns_default_when_no_entity_matches(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Act
                    var entity = database.FindSingle<Dog>($"WHERE Name LIKE CONCAT({"Some Name"}, '%') and Age = {10}");

                    // Assert
                    entity.Should().BeNull();
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
                    Action act = () => database.FindSingle<Dog>($"WHERE Age = {10}");

                    // Assert
                    act.ShouldThrow<InvalidOperationException>();
                }
            }
        }

        public class FindSingleWhereObject
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
                    var entity = database.FindSingle<Dog>(new { Age = 10 });

                    // Assert
                    entity.ShouldBeEquivalentTo(new Dog { Name = "Some Name 1", Age = 10 }, o => o.Excluding(e => e.Id));
                }
            }

            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Returns_default_when_no_entity_matches(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Act
                    var entity = database.FindSingle<Dog>(new { Age = 10 });

                    // Assert
                    entity.Should().BeNull();
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
                    Action act = () => database.FindSingle<Dog>(new { Age = 10 });

                    // Assert
                    act.ShouldThrow<InvalidOperationException>();
                }
            }
        }
    }
}