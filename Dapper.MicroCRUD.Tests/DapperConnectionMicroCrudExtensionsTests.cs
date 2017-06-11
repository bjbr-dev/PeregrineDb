// <copyright file="DbConnectionExtensionsTests.cs" company="Berkeleybross">
//   Copyright (c) Berkeleybross. All rights reserved.
// </copyright>
namespace Dapper.MicroCRUD.Tests
{
    using System;
    using System.Linq;
    using Dapper.MicroCRUD.Databases;
    using Dapper.MicroCRUD.Schema;
    using Dapper.MicroCRUD.Tests.Dialects.Postgres;
    using Dapper.MicroCRUD.Tests.Dialects.SqlServer;
    using Dapper.MicroCRUD.Tests.ExampleEntities;
    using FluentAssertions;
    using Pagination;
    using Xunit;

    public abstract class DapperConnectionMicroCrudExtensionsTests
    {
        private readonly IDatabase database;

        protected DapperConnectionMicroCrudExtensionsTests(DatabaseFixture fixture)
        {
            this.database = fixture?.DefaultDatabase;
        }

        public class Misc
            : DapperConnectionMicroCrudExtensionsTests
        {
            public Misc()
                : base(null)
            {
            }

            [Fact]
            public void Is_in_same_namespace_as_dapper()
            {
                // Assert
                var dapperType = typeof(SqlMapper);
                typeof(DbConnectionExtensions).Namespace.Should().Be(dapperType.Namespace);
                typeof(DapperConnectionExtensions).Namespace.Should().Be(dapperType.Namespace);
                typeof(DapperConnectionMicroCrudExtensions).Namespace.Should().Be(dapperType.Namespace);
            }
        }

        public abstract class Count
            : DapperConnectionMicroCrudExtensionsTests
        {
            protected Count(DatabaseFixture fixture)
                : base(fixture)
            {
            }

            [Fact]
            public void Counts_all_entities_when_conditions_is_null()
            {
                // Arrange
                this.database.Insert(new User { Name = "Some Name 1", Age = 10 });
                this.database.Insert(new User { Name = "Some Name 2", Age = 10 });
                this.database.Insert(new User { Name = "Some Name 3", Age = 10 });
                this.database.Insert(new User { Name = "Some Name 4", Age = 11 });

                // Act
                var result = this.database.Count<User>();

                // Assert
                result.Should().Be(4);

                // Cleanup
                this.database.DeleteAll<User>();
            }

            [Fact]
            public void Counts_entities_matching_conditions()
            {
                // Arrange
                this.database.Insert(new User { Name = "Some Name 1", Age = 10 });
                this.database.Insert(new User { Name = "Some Name 2", Age = 10 });
                this.database.Insert(new User { Name = "Some Name 3", Age = 10 });
                this.database.Insert(new User { Name = "Some Name 4", Age = 11 });

                // Act
                var result = this.database.Count<User>("WHERE Age < @Age", new { Age = 11 });

                // Assert
                result.Should().Be(3);

                // Cleanup
                this.database.DeleteAll<User>();
            }

            [Fact]
            public void Counts_entities_in_alternate_schema()
            {
                this.database.Insert<int>(new SchemaOther { Name = "Some Name" });
                this.database.Insert<int>(new SchemaOther { Name = "Some Name" });
                this.database.Insert<int>(new SchemaOther { Name = "Some Name" });
                this.database.Insert<int>(new SchemaOther { Name = "Some Name" });

                // Act
                var result = this.database.Count<SchemaOther>();

                // Assert
                result.Should().Be(4);

                // Cleanup
                this.database.DeleteAll<SchemaOther>();
            }

            [Collection(nameof(PostgresCollection))]
            public class Postgres
                : Count
            {
                public Postgres(PostgresFixture fixture)
                    : base(fixture)
                {
                }
            }

            [Collection(nameof(SqlServerCollection))]
            public class SqlServer
                : Count
            {
                public SqlServer(SqlServerFixture fixture)
                    : base(fixture)
                {
                }
            }
        }

        public abstract class CountWhereObject
            : DapperConnectionMicroCrudExtensionsTests
        {
            protected CountWhereObject(DatabaseFixture fixture)
                : base(fixture)
            {
            }

            [Fact]
            public void Throws_exception_when_conditions_is_null()
            {
                // Act
                Assert.Throws<ArgumentNullException>(() => this.database.Count<User>((object)null));
            }

            [Fact]
            public void Counts_all_entities_when_conditions_is_empty()
            {
                // Arrange
                this.database.Insert(new User { Name = "Some Name 1", Age = 10 });
                this.database.Insert(new User { Name = "Some Name 2", Age = 10 });
                this.database.Insert(new User { Name = "Some Name 3", Age = 10 });
                this.database.Insert(new User { Name = "Some Name 4", Age = 11 });

                // Act
                var result = this.database.Count<User>(new { });

                // Assert
                result.Should().Be(4);

                // Cleanup
                this.database.DeleteAll<User>();
            }

            [Fact]
            public void Counts_entities_matching_conditions()
            {
                // Arrange
                this.database.Insert(new User { Name = "Some Name 1", Age = 10 });
                this.database.Insert(new User { Name = "Some Name 2", Age = 10 });
                this.database.Insert(new User { Name = "Some Name 3", Age = 10 });
                this.database.Insert(new User { Name = "Some Name 4", Age = 11 });

                // Act
                var result = this.database.Count<User>(new { Age = 10 });

                // Assert
                result.Should().Be(3);

                // Cleanup
                this.database.DeleteAll<User>();
            }

            [Collection(nameof(PostgresCollection))]
            public class Postgres
                : CountWhereObject
            {
                public Postgres(PostgresFixture fixture)
                    : base(fixture)
                {
                }
            }

            [Collection(nameof(SqlServerCollection))]
            public class SqlServer
                : CountWhereObject
            {
                public SqlServer(SqlServerFixture fixture)
                    : base(fixture)
                {
                }
            }
        }

        public abstract class Find
            : DapperConnectionMicroCrudExtensionsTests
        {
            protected Find(DatabaseFixture fixture)
                : base(fixture)
            {
            }

            [Fact]
            public void Throws_exception_when_entity_has_no_key()
            {
                // Arrange
                this.database.Insert(new NoKey { Name = "Some Name", Age = 1 });

                // Act
                Assert.Throws<InvalidPrimaryKeyException>(
                    () => this.database.Find<NoKey>("Some Name"));
            }

            [Fact]
            public void Returns_null_when_entity_is_not_found()
            {
                // Act
                var entity = this.database.Find<KeyInt32>(12);

                // Assert
                entity.Should().Be(null);
            }

            [Fact]
            public void Finds_entity_by_Int32_primary_key()
            {
                // Arrange
                var id = this.database.Insert<int>(new KeyInt32 { Name = "Some Name" });

                // Act
                var entity = this.database.Find<KeyInt32>(id);

                // Assert
                entity.Name.Should().Be("Some Name");

                // Cleanup
                this.database.Delete<KeyInt32>(id);
            }

            [Fact]
            public void Finds_entity_by_Int64_primary_key()
            {
                // Arrange
                var id = this.database.Insert<long>(new KeyInt64 { Name = "Some Name" });

                // Act
                var user = this.database.Find<KeyInt64>(id);

                // Assert
                user.Name.Should().Be("Some Name");

                // Cleanup
                this.database.Delete<KeyInt64>(id);
            }

            [Fact]
            public void Finds_entity_by_string_primary_key()
            {
                // Arrange
                this.database.Insert(new KeyString { Name = "Some Name", Age = 42 });

                // Act
                var entity = this.database.Find<KeyString>("Some Name");

                // Assert
                entity.Age.Should().Be(42);

                // Cleanup
                this.database.Delete(entity);
            }

            [Fact]
            public void Finds_entity_by_guid_primary_key()
            {
                // Arrange
                var id = Guid.NewGuid();
                this.database.Insert(new KeyGuid { Id = id, Name = "Some Name" });

                // Act
                var entity = this.database.Find<KeyGuid>(id);

                // Assert
                entity.Name.Should().Be("Some Name");

                // Cleanup
                this.database.Delete(entity);
            }

            [Fact]
            public void Finds_entity_by_composite_key()
            {
                // Arrange
                this.database.Insert(new CompositeKeys { Key1 = 1, Key2 = 1, Name = "Some Name" });
                var id = new { Key1 = 1, Key2 = 1 };

                // Act
                var entity = this.database.Find<CompositeKeys>(id);

                // Assert
                entity.Name.Should().Be("Some Name");

                // Cleanup
                this.database.DeleteAll<CompositeKeys>();
            }

            [Fact]
            public void Finds_entities_in_alternate_schema()
            {
                // Arrange
                var id = this.database.Insert<int>(new SchemaOther { Name = "Some Name" });

                // Act
                var entity = this.database.Find<SchemaOther>(id);

                // Assert
                entity.Name.Should().Be("Some Name");

                // Cleanup
                this.database.Delete<SchemaOther>(id);
            }

            [Fact]
            public void Finds_entities_with_enum_property()
            {
                // Arrange
                var id = this.database.Insert<int>(new PropertyEnum { FavoriteColor = Color.Green });

                // Act
                var entity = this.database.Find<PropertyEnum>(id);

                // Assert
                entity.FavoriteColor.Should().Be(Color.Green);

                // Cleanup
                this.database.Delete<PropertyEnum>(id);
            }

            [Fact]
            public void Finds_entities_with_all_possible_types()
            {
                // Arrange
                var id = this.database.Insert<int>(
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
                var entity = this.database.Find<PropertyAllPossibleTypes>(id);

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
                this.database.Delete<PropertyAllPossibleTypes>(id);
            }

            [Fact]
            public void Ignores_columns_which_are_not_mapped()
            {
                // Arrange
                var id = this.database.Insert<int>(new PropertyNotMapped { Firstname = "Bobby", LastName = "DropTables", Age = 10 });

                // Act
                var entity = this.database.Find<PropertyNotMapped>(id);

                // Assert
                entity.Firstname.Should().Be("Bobby");
                entity.LastName.Should().Be("DropTables");
                entity.FullName.Should().Be("Bobby DropTables");
                entity.Age.Should().Be(0);

                // Cleanup
                this.database.DeleteAll<PropertyNotMapped>();
            }

            [Collection(nameof(PostgresCollection))]
            public class Postgres
                : Find
            {
                public Postgres(PostgresFixture fixture)
                    : base(fixture)
                {
                }
            }

            [Collection(nameof(SqlServerCollection))]
            public class SqlServer
                : Find
            {
                public SqlServer(SqlServerFixture fixture)
                    : base(fixture)
                {
                }
            }
        }

        public abstract class Get
            : DapperConnectionMicroCrudExtensionsTests
        {
            protected Get(DatabaseFixture fixture)
                : base(fixture)
            {
            }

            [Fact]
            public void Throws_exception_when_entity_has_no_key()
            {
                // Arrange
                this.database.Insert(new NoKey { Name = "Some Name", Age = 1 });

                // Act
                Assert.Throws<InvalidPrimaryKeyException>(
                    () => this.database.Get<NoKey>("Some Name"));
            }

            [Fact]
            public void Throws_exception_when_entity_is_not_found()
            {
                // Act
                Assert.Throws<InvalidOperationException>(
                    () => this.database.Get<KeyInt32>(5));
            }

            [Fact]
            public void Finds_entity_by_Int32_primary_key()
            {
                // Arrange
                var id = this.database.Insert<int>(new KeyInt32 { Name = "Some Name" });

                // Act
                var entity = this.database.Get<KeyInt32>(id);

                // Assert
                entity.Name.Should().Be("Some Name");

                // Cleanup
                this.database.Delete<KeyInt32>(id);
            }

            [Fact]
            public void Finds_entity_by_Int64_primary_key()
            {
                // Arrange
                var id = this.database.Insert<long>(new KeyInt64 { Name = "Some Name" });

                // Act
                var user = this.database.Get<KeyInt64>(id);

                // Assert
                user.Name.Should().Be("Some Name");

                // Cleanup
                this.database.Delete<KeyInt64>(id);
            }

            [Fact]
            public void Finds_entity_by_string_primary_key()
            {
                // Arrange
                this.database.Insert(new KeyString { Name = "Some Name", Age = 42 });

                // Act
                var entity = this.database.Get<KeyString>("Some Name");

                // Assert
                entity.Age.Should().Be(42);

                // Cleanup
                this.database.Delete(entity);
            }

            [Fact]
            public void Finds_entity_by_guid_primary_key()
            {
                // Arrange
                var id = Guid.NewGuid();
                this.database.Insert(new KeyGuid { Id = id, Name = "Some Name" });

                // Act
                var entity = this.database.Get<KeyGuid>(id);

                // Assert
                entity.Name.Should().Be("Some Name");

                // Cleanup
                this.database.Delete(entity);
            }

            [Fact]
            public void Finds_entity_by_composite_key()
            {
                // Arrange
                this.database.Insert(new CompositeKeys { Key1 = 1, Key2 = 1, Name = "Some Name" });
                var id = new { Key1 = 1, Key2 = 1 };

                // Act
                var entity = this.database.Get<CompositeKeys>(id);

                // Assert
                entity.Name.Should().Be("Some Name");

                // Cleanup
                this.database.DeleteAll<CompositeKeys>();
            }

            [Fact]
            public void Finds_entities_in_alternate_schema()
            {
                // Arrange
                var id = this.database.Insert<int>(new SchemaOther { Name = "Some Name" });

                // Act
                var entity = this.database.Get<SchemaOther>(id);

                // Assert
                entity.Name.Should().Be("Some Name");

                // Cleanup
                this.database.Delete<SchemaOther>(id);
            }

            [Fact]
            public void Ignores_columns_which_are_not_mapped()
            {
                // Arrange
                var id = this.database.Insert<int>(new PropertyNotMapped { Firstname = "Bobby", LastName = "DropTables", Age = 10 });

                // Act
                var entity = this.database.Get<PropertyNotMapped>(id);

                // Assert
                entity.Firstname.Should().Be("Bobby");
                entity.LastName.Should().Be("DropTables");
                entity.FullName.Should().Be("Bobby DropTables");
                entity.Age.Should().Be(0);

                // Cleanup
                this.database.DeleteAll<PropertyNotMapped>();
            }

            [Collection(nameof(PostgresCollection))]
            public class Postgres
                : Get
            {
                public Postgres(PostgresFixture fixture)
                    : base(fixture)
                {
                }
            }

            [Collection(nameof(SqlServerCollection))]
            public class SqlServer
                : Get
            {
                public SqlServer(SqlServerFixture fixture)
                    : base(fixture)
                {
                }
            }
        }

        public abstract class GetFirstOrDefault
            : DapperConnectionMicroCrudExtensionsTests
        {
            protected GetFirstOrDefault(DatabaseFixture fixture)
                : base(fixture)
            {
            }

            [Fact]
            public void Gets_first_matching_result()
            {
                // Arrange
                this.database.Insert(new User { Name = "Some Name 1", Age = 10 });
                this.database.Insert(new User { Name = "Some Name 2", Age = 10 });
                this.database.Insert(new User { Name = "Some Name 3", Age = 10 });
                this.database.Insert(new User { Name = "Some Name 4", Age = 11 });

                // Act
                var user = this.database.GetFirstOrDefault<User>(
                    "WHERE Name LIKE CONCAT(@Search, '%') and Age = @Age",
                    "Name DESC",
                    new { Search = "Some Name", Age = 10 });

                // Assert
                user.ShouldBeEquivalentTo(new { Name = "Some Name 1", Age = 10 });

                // Cleanup
                this.database.DeleteAll<User>();
            }

            [Fact]
            public void Returns_default_when_no_entity_matches()
            {
                // Act
                var user = this.database.GetFirstOrDefault<User>(
                    "WHERE Name LIKE CONCAT(@Search, '%') and Age = @Age",
                    "Name DESC",
                    new { Search = "Some Name", Age = 10 });

                // Assert
                user.Should().BeNull();
            }

            [Collection(nameof(PostgresCollection))]
            public class Postgres
                : GetFirstOrDefault
            {
                public Postgres(PostgresFixture fixture)
                    : base(fixture)
                {
                }
            }

            [Collection(nameof(SqlServerCollection))]
            public class SqlServer
                : GetFirstOrDefault
            {
                public SqlServer(SqlServerFixture fixture)
                    : base(fixture)
                {
                }
            }
        }

        public abstract class GetFirstOrDefaultWhereObject
            : DapperConnectionMicroCrudExtensionsTests
        {
            protected GetFirstOrDefaultWhereObject(DatabaseFixture fixture)
                : base(fixture)
            {
            }

            [Fact]
            public void Gets_first_matching_result()
            {
                // Arrange
                this.database.Insert(new User { Name = "Some Name 1", Age = 10 });
                this.database.Insert(new User { Name = "Some Name 2", Age = 10 });
                this.database.Insert(new User { Name = "Some Name 3", Age = 10 });
                this.database.Insert(new User { Name = "Some Name 4", Age = 11 });

                // Act
                var user = this.database.GetFirstOrDefault<User>(new { Age = 10 }, "Name DESC");

                // Assert
                user.ShouldBeEquivalentTo(new { Name = "Some Name 1", Age = 10 });

                // Cleanup
                this.database.DeleteAll<User>();
            }

            [Fact]
            public void Returns_default_when_no_entity_matches()
            {
                // Act
                var user = this.database.GetFirstOrDefault<User>(new { Age = 10 }, "Name DESC");

                // Assert
                user.Should().BeNull();
            }

            [Collection(nameof(PostgresCollection))]
            public class Postgres
                : GetFirstOrDefaultWhereObject
            {
                public Postgres(PostgresFixture fixture)
                    : base(fixture)
                {
                }
            }

            [Collection(nameof(SqlServerCollection))]
            public class SqlServer
                : GetFirstOrDefaultWhereObject
            {
                public SqlServer(SqlServerFixture fixture)
                    : base(fixture)
                {
                }
            }
        }

        public abstract class GetFirst
            : DapperConnectionMicroCrudExtensionsTests
        {
            protected GetFirst(DatabaseFixture fixture)
                : base(fixture)
            {
            }

            [Fact]
            public void Gets_first_matching_result()
            {
                // Arrange
                this.database.Insert(new User { Name = "Some Name 1", Age = 10 });
                this.database.Insert(new User { Name = "Some Name 2", Age = 10 });
                this.database.Insert(new User { Name = "Some Name 3", Age = 10 });
                this.database.Insert(new User { Name = "Some Name 4", Age = 11 });

                // Act
                var user = this.database.GetFirst<User>(
                    "WHERE Name LIKE CONCAT(@Search, '%') and Age = @Age",
                    "Name DESC",
                    new { Search = "Some Name", Age = 10 });

                // Assert
                user.ShouldBeEquivalentTo(new { Name = "Some Name 1", Age = 10 });

                // Cleanup
                this.database.DeleteAll<User>();
            }

            [Fact]
            public void Throws_exception_when_no_entity_matches()
            {
                // Act
                Action act = () => this.database.GetFirst<User>(
                    "WHERE Name LIKE CONCAT(@Search, '%') and Age = @Age",
                    "Name DESC",
                    new { Search = "Some Name", Age = 10 });

                // Assert
                act.ShouldThrow<InvalidOperationException>();
            }

            [Collection(nameof(PostgresCollection))]
            public class Postgres
                : GetFirst
            {
                public Postgres(PostgresFixture fixture)
                    : base(fixture)
                {
                }
            }

            [Collection(nameof(SqlServerCollection))]
            public class SqlServer
                : GetFirst
            {
                public SqlServer(SqlServerFixture fixture)
                    : base(fixture)
                {
                }
            }
        }

        public abstract class GetFirstWhereObject
            : DapperConnectionMicroCrudExtensionsTests
        {
            protected GetFirstWhereObject(DatabaseFixture fixture)
                : base(fixture)
            {
            }

            [Fact]
            public void Gets_first_matching_result()
            {
                // Arrange
                this.database.Insert(new User { Name = "Some Name 1", Age = 10 });
                this.database.Insert(new User { Name = "Some Name 2", Age = 10 });
                this.database.Insert(new User { Name = "Some Name 3", Age = 10 });
                this.database.Insert(new User { Name = "Some Name 4", Age = 11 });

                // Act
                var user = this.database.GetFirst<User>(new { Age = 10 }, "Name DESC");

                // Assert
                user.ShouldBeEquivalentTo(new { Name = "Some Name 1", Age = 10 });

                // Cleanup
                this.database.DeleteAll<User>();
            }

            [Fact]
            public void Throws_exception_when_no_entity_matches()
            {
                // Act
                Action act = () => this.database.GetFirst<User>(new { Age = 10 }, "Name DESC");

                // Assert
                act.ShouldThrow<InvalidOperationException>();
            }

            [Collection(nameof(PostgresCollection))]
            public class Postgres
                : GetFirstWhereObject
            {
                public Postgres(PostgresFixture fixture)
                    : base(fixture)
                {
                }
            }

            [Collection(nameof(SqlServerCollection))]
            public class SqlServer
                : GetFirstWhereObject
            {
                public SqlServer(SqlServerFixture fixture)
                    : base(fixture)
                {
                }
            }
        }

        public abstract class GetSingleOrDefault
            : DapperConnectionMicroCrudExtensionsTests
        {
            protected GetSingleOrDefault(DatabaseFixture fixture)
                : base(fixture)
            {
            }

            [Fact]
            public void Gets_only_matching_result()
            {
                // Arrange
                this.database.Insert(new User { Name = "Some Name 1", Age = 10 });
                this.database.Insert(new User { Name = "Some Name 2", Age = 10 });
                this.database.Insert(new User { Name = "Some Name 3", Age = 10 });
                this.database.Insert(new User { Name = "Some Name 4", Age = 11 });

                // Act
                var user = this.database.GetSingleOrDefault<User>(
                    "WHERE Name LIKE CONCAT(@Search, '%') and Age = @Age",
                    new { Search = "Some Name", Age = 10 });

                // Assert
                user.ShouldBeEquivalentTo(new { Name = "Some Name 1", Age = 10 });

                // Cleanup
                this.database.DeleteAll<User>();
            }

            [Fact]
            public void Returns_default_when_no_entity_matches()
            {
                // Act
                var user = this.database.GetSingleOrDefault<User>(
                    "WHERE Name LIKE CONCAT(@Search, '%') and Age = @Age",
                    new { Search = "Some Name", Age = 10 });

                // Assert
                user.Should().BeNull();
            }

            [Fact]
            public void Throws_exception_when_multiple_entities_match()
            {
                // Arrange
                this.database.Insert(new User { Name = "Some Name 1", Age = 10 });
                this.database.Insert(new User { Name = "Some Name 2", Age = 10 });

                // Act
                Action act = () => this.database.GetSingleOrDefault<User>("Age = @Age", new { Age = 10 });

                // Assert
                act.ShouldThrow<InvalidOperationException>();

                // Cleanup
                this.database.DeleteAll<User>();
            }

            [Collection(nameof(PostgresCollection))]
            public class Postgres
                : GetSingleOrDefault
            {
                public Postgres(PostgresFixture fixture)
                    : base(fixture)
                {
                }
            }

            [Collection(nameof(SqlServerCollection))]
            public class SqlServer
                : GetSingleOrDefault
            {
                public SqlServer(SqlServerFixture fixture)
                    : base(fixture)
                {
                }
            }
        }

        public abstract class GetSingleOrDefaultWhereObject
            : DapperConnectionMicroCrudExtensionsTests
        {
            protected GetSingleOrDefaultWhereObject(DatabaseFixture fixture)
                : base(fixture)
            {
            }

            [Fact]
            public void Gets_first_matching_result()
            {
                // Arrange
                this.database.Insert(new User { Name = "Some Name 1", Age = 10 });
                this.database.Insert(new User { Name = "Some Name 2", Age = 10 });
                this.database.Insert(new User { Name = "Some Name 3", Age = 10 });
                this.database.Insert(new User { Name = "Some Name 4", Age = 11 });

                // Act
                var user = this.database.GetSingleOrDefault<User>(new { Age = 10 });

                // Assert
                user.ShouldBeEquivalentTo(new { Name = "Some Name 1", Age = 10 });

                // Cleanup
                this.database.DeleteAll<User>();
            }

            [Fact]
            public void Returns_default_when_no_entity_matches()
            {
                // Act
                var user = this.database.GetSingleOrDefault<User>(new { Age = 10 });

                // Assert
                user.Should().BeNull();
            }

            [Fact]
            public void Throws_exception_when_multiple_entities_match()
            {
                // Arrange
                this.database.Insert(new User { Name = "Some Name 1", Age = 10 });
                this.database.Insert(new User { Name = "Some Name 2", Age = 10 });

                // Act
                Action act = () => this.database.GetSingleOrDefault<User>(new { Age = 10 });

                // Assert
                act.ShouldThrow<InvalidOperationException>();

                // Cleanup
                this.database.DeleteAll<User>();
            }

            [Collection(nameof(PostgresCollection))]
            public class Postgres
                : GetSingleOrDefaultWhereObject
            {
                public Postgres(PostgresFixture fixture)
                    : base(fixture)
                {
                }
            }

            [Collection(nameof(SqlServerCollection))]
            public class SqlServer
                : GetSingleOrDefaultWhereObject
            {
                public SqlServer(SqlServerFixture fixture)
                    : base(fixture)
                {
                }
            }
        }

        public abstract class GetSingle
            : DapperConnectionMicroCrudExtensionsTests
        {
            protected GetSingle(DatabaseFixture fixture)
                : base(fixture)
            {
            }

            [Fact]
            public void Gets_first_matching_result()
            {
                // Arrange
                this.database.Insert(new User { Name = "Some Name 1", Age = 10 });
                this.database.Insert(new User { Name = "Some Name 2", Age = 10 });
                this.database.Insert(new User { Name = "Some Name 3", Age = 10 });
                this.database.Insert(new User { Name = "Some Name 4", Age = 11 });

                // Act
                var user = this.database.GetSingle<User>(
                    "WHERE Name LIKE CONCAT(@Search, '%') and Age = @Age",
                    new { Search = "Some Name", Age = 10 });

                // Assert
                user.ShouldBeEquivalentTo(new { Name = "Some Name 1", Age = 10 });

                // Cleanup
                this.database.DeleteAll<User>();
            }

            [Fact]
            public void Throws_exception_when_no_entity_matches()
            {
                // Act
                Action act = () => this.database.GetSingle<User>(
                    "WHERE Name LIKE CONCAT(@Search, '%') and Age = @Age",
                    new { Search = "Some Name", Age = 10 });

                // Assert
                act.ShouldThrow<InvalidOperationException>();
            }

            [Fact]
            public void Throws_exception_when_multiple_entities_match()
            {
                // Arrange
                this.database.Insert(new User { Name = "Some Name 1", Age = 10 });
                this.database.Insert(new User { Name = "Some Name 2", Age = 10 });

                // Act
                Action act = () => this.database.GetSingle<User>("WHERE Age = @Age", new { Age = 10 });

                // Assert
                act.ShouldThrow<InvalidOperationException>();

                // Cleanup
                this.database.DeleteAll<User>();
            }

            [Collection(nameof(PostgresCollection))]
            public class Postgres
                : GetSingle
            {
                public Postgres(PostgresFixture fixture)
                    : base(fixture)
                {
                }
            }

            [Collection(nameof(SqlServerCollection))]
            public class SqlServer
                : GetSingle
            {
                public SqlServer(SqlServerFixture fixture)
                    : base(fixture)
                {
                }
            }
        }

        public abstract class GetSingleWhereObject
            : DapperConnectionMicroCrudExtensionsTests
        {
            protected GetSingleWhereObject(DatabaseFixture fixture)
                : base(fixture)
            {
            }

            [Fact]
            public void Gets_first_matching_result()
            {
                // Arrange
                this.database.Insert(new User { Name = "Some Name 1", Age = 10 });
                this.database.Insert(new User { Name = "Some Name 2", Age = 10 });
                this.database.Insert(new User { Name = "Some Name 3", Age = 10 });
                this.database.Insert(new User { Name = "Some Name 4", Age = 11 });

                // Act
                var user = this.database.GetSingle<User>(new { Age = 10 });

                // Assert
                user.ShouldBeEquivalentTo(new { Name = "Some Name 1", Age = 10 });

                // Cleanup
                this.database.DeleteAll<User>();
            }

            [Fact]
            public void Throws_exception_when_no_entity_matches()
            {
                // Act
                Action act = () => this.database.GetSingle<User>(new { Age = 10 });

                // Assert
                act.ShouldThrow<InvalidOperationException>();
            }

            [Fact]
            public void Throws_exception_when_multiple_entities_match()
            {
                // Arrange
                this.database.Insert(new User { Name = "Some Name 1", Age = 10 });
                this.database.Insert(new User { Name = "Some Name 2", Age = 10 });

                // Act
                Action act = () => this.database.GetSingle<User>(new { Age = 10 });

                // Assert
                act.ShouldThrow<InvalidOperationException>();

                // Cleanup
                this.database.DeleteAll<User>();
            }

            [Collection(nameof(PostgresCollection))]
            public class Postgres
                : GetSingleWhereObject
            {
                public Postgres(PostgresFixture fixture)
                    : base(fixture)
                {
                }
            }

            [Collection(nameof(SqlServerCollection))]
            public class SqlServer
                : GetSingleWhereObject
            {
                public SqlServer(SqlServerFixture fixture)
                    : base(fixture)
                {
                }
            }
        }

        public abstract class GetRange
            : DapperConnectionMicroCrudExtensionsTests
        {
            protected GetRange(DatabaseFixture fixture)
                : base(fixture)
            {
            }

            [Fact]
            public void Filters_result_by_conditions()
            {
                // Arrange
                this.database.Insert(new User { Name = "Some Name 1", Age = 10 });
                this.database.Insert(new User { Name = "Some Name 2", Age = 10 });
                this.database.Insert(new User { Name = "Some Name 3", Age = 10 });
                this.database.Insert(new User { Name = "Some Name 4", Age = 11 });

                // Act
                var users = this.database.GetRange<User>(
                    "WHERE Name LIKE CONCAT(@Search, '%') and Age = @Age",
                    new { Search = "Some Name", Age = 10 });

                // Assert
                users.Count().Should().Be(3);

                // Cleanup
                this.database.DeleteAll<User>();
            }

            [Fact]
            public void Returns_everything_when_conditions_is_null()
            {
                // Arrange
                this.database.Insert(new User { Name = "Some Name 1", Age = 10 });
                this.database.Insert(new User { Name = "Some Name 2", Age = 10 });
                this.database.Insert(new User { Name = "Some Name 3", Age = 10 });
                this.database.Insert(new User { Name = "Some Name 4", Age = 11 });

                // Act
                var users = this.database.GetRange<User>(null);

                // Assert
                users.Count().Should().Be(4);

                // Cleanup
                this.database.DeleteAll<User>();
            }

            [Collection(nameof(PostgresCollection))]
            public class Postgres
                : GetRange
            {
                public Postgres(PostgresFixture fixture)
                    : base(fixture)
                {
                }
            }

            [Collection(nameof(SqlServerCollection))]
            public class SqlServer
                : GetRange
            {
                public SqlServer(SqlServerFixture fixture)
                    : base(fixture)
                {
                }
            }
        }

        public abstract class GetRangeWhereObject
            : DapperConnectionMicroCrudExtensionsTests
        {
            protected GetRangeWhereObject(DatabaseFixture fixture)
                : base(fixture)
            {
            }

            [Fact]
            public void Throws_exception_when_conditions_is_null()
            {
                // Act
                Assert.Throws<ArgumentNullException>(() => this.database.GetRange<User>((object)null));
            }

            [Fact]
            public void Returns_all_when_conditions_is_empty()
            {
                // Arrange
                this.database.Insert(new User { Name = "Some Name 1", Age = 10 });
                this.database.Insert(new User { Name = "Some Name 2", Age = 10 });
                this.database.Insert(new User { Name = "Some Name 3", Age = 10 });
                this.database.Insert(new User { Name = "Some Name 4", Age = 11 });

                // Act
                var users = this.database.GetRange<User>(new { });

                // Assert
                users.Count().Should().Be(4);

                // Cleanup
                this.database.DeleteAll<User>();
            }

            [Fact]
            public void Filters_result_by_conditions()
            {
                // Arrange
                this.database.Insert(new User { Name = "Some Name 1", Age = 10 });
                this.database.Insert(new User { Name = "Some Name 2", Age = 10 });
                this.database.Insert(new User { Name = "Some Name 3", Age = 10 });
                this.database.Insert(new User { Name = "Some Name 4", Age = 11 });

                // Act
                var users = this.database.GetRange<User>(new { Age = 10 });

                // Assert
                users.Count().Should().Be(3);

                // Cleanup
                this.database.DeleteAll<User>();
            }

            [Fact]
            public void Matches_column_name_case_insensitively()
            {
                // Arrange
                this.database.Insert(new User { Name = "Some Name 1", Age = 10 });
                this.database.Insert(new User { Name = "Some Name 2", Age = 10 });
                this.database.Insert(new User { Name = "Some Name 3", Age = 10 });
                this.database.Insert(new User { Name = "Some Name 4", Age = 11 });

                // Act
                var users = this.database.GetRange<User>(new { age = 10 });

                // Assert
                users.Count().Should().Be(3);

                // Cleanup
                this.database.DeleteAll<User>();
            }

            [Fact]
            public void Throws_exception_when_column_not_found()
            {
                // Act
                Action act = () => this.database.GetRange<User>(new { Ages = 10 });

                // Assert
                act.ShouldThrow<InvalidConditionSchemaException>();
            }

            [Fact]
            public void When_value_is_not_null_does_not_find_nulls()
            {
                // Arrange
                this.database.Insert(new PropertyNullable { Name = null });
                this.database.Insert(new PropertyNullable { Name = "Some Name 3" });
                this.database.Insert(new PropertyNullable { Name = null });

                // Act
                var users = this.database.GetRange<PropertyNullable>(new { Name = "Some Name 3" });

                // Assert
                users.Count().Should().Be(1);

                // Cleanup
                this.database.DeleteAll<PropertyNullable>();
            }

            [Fact]
            public void When_value_is_null_finds_nulls()
            {
                // Arrange
                this.database.Insert(new PropertyNullable { Name = null });
                this.database.Insert(new PropertyNullable { Name = "Some Name 3" });
                this.database.Insert(new PropertyNullable { Name = null });

                // Act
                var users = this.database.GetRange<PropertyNullable>(new { Name = (string)null });

                // Assert
                users.Count().Should().Be(2);

                // Cleanup
                this.database.DeleteAll<PropertyNullable>();
            }

            [Fact]
            public void Filters_on_multiple_properties()
            {
                // Arrange
                this.database.Insert(new User { Name = "Some Name 1", Age = 10 });
                this.database.Insert(new User { Name = "Some Name 2", Age = 10 });
                this.database.Insert(new User { Name = "Some Name 2", Age = 12 });

                // Act
                var users = this.database.GetRange<User>(new { Name = "Some Name 2", Age = 10 });

                // Assert
                users.Count().Should().Be(1);

                // Cleanup
                this.database.DeleteAll<User>();
            }

            [Collection(nameof(PostgresCollection))]
            public class Postgres
                : GetRangeWhereObject
            {
                public Postgres(PostgresFixture fixture)
                    : base(fixture)
                {
                }
            }

            [Collection(nameof(SqlServerCollection))]
            public class SqlServer
                : GetRangeWhereObject
            {
                public SqlServer(SqlServerFixture fixture)
                    : base(fixture)
                {
                }
            }
        }

        public abstract class GetPage
            : DapperConnectionMicroCrudExtensionsTests
        {
            protected GetPage(DatabaseFixture fixture)
                : base(fixture)
            {
            }

            [Fact]
            public void Returns_empty_list_when_there_are_no_entities()
            {
                // Act
                var pageBuilder = new PageIndexPageBuilder(1, 10);
                var users = this.database.GetPage<User>(pageBuilder, null, "Age");

                // Assert
                users.Items.Count().Should().Be(0);
            }

            [Fact]
            public void Filters_result_by_conditions()
            {
                // Arrange
                this.database.Insert(new User { Name = "Some Name 1", Age = 10 });
                this.database.Insert(new User { Name = "Some Name 2", Age = 10 });
                this.database.Insert(new User { Name = "Some Name 3", Age = 10 });
                this.database.Insert(new User { Name = "Some Name 4", Age = 11 });

                // Act
                var users = this.database.GetPage<User>(
                    new PageIndexPageBuilder(1, 10),
                    "WHERE Name LIKE CONCAT(@Search, '%') and Age = @Age",
                    "Age",
                    new { Search = "Some Name", Age = 10 });

                // Assert
                users.Items.Count().Should().Be(3);

                // Cleanup
                this.database.DeleteAll<User>();
            }

            [Fact]
            public void Gets_first_page()
            {
                // Arrange
                this.database.Insert(new User { Name = "Some Name 1", Age = 10 });
                this.database.Insert(new User { Name = "Some Name 2", Age = 10 });
                this.database.Insert(new User { Name = "Some Name 3", Age = 10 });
                this.database.Insert(new User { Name = "Some Name 4", Age = 11 });

                // Act
                var users = this.database.GetPage<User>(
                    new PageIndexPageBuilder(1, 2),
                    "WHERE Name LIKE CONCAT(@Search, '%') and Age = @Age",
                    "Age DESC",
                    new { Search = "Some Name", Age = 10 }).Items;

                // Assert
                users.Count().Should().Be(2);
                users[0].Name.Should().Be("Some Name 1");
                users[1].Name.Should().Be("Some Name 2");

                // Cleanup
                this.database.DeleteAll<User>();
            }

            [Fact]
            public void Gets_second_page()
            {
                // Arrange
                this.database.Insert(new User { Name = "Some Name 1", Age = 10 });
                this.database.Insert(new User { Name = "Some Name 2", Age = 10 });
                this.database.Insert(new User { Name = "Some Name 3", Age = 10 });
                this.database.Insert(new User { Name = "Some Name 4", Age = 11 });

                // Act
                var users = this.database.GetPage<User>(
                    new PageIndexPageBuilder(2, 2),
                    "WHERE Name LIKE CONCAT(@Search, '%') and Age = @Age",
                    "Age DESC",
                    new { Search = "Some Name", Age = 10 }).Items;

                // Assert
                users.Count().Should().Be(1);
                users[0].Name.Should().Be("Some Name 3");

                // Cleanup
                this.database.DeleteAll<User>();
            }

            [Fact]
            public void Returns_empty_set_past_last_page()
            {
                // Arrange
                this.database.Insert(new User { Name = "Some Name 1", Age = 10 });
                this.database.Insert(new User { Name = "Some Name 2", Age = 10 });
                this.database.Insert(new User { Name = "Some Name 3", Age = 10 });
                this.database.Insert(new User { Name = "Some Name 4", Age = 11 });

                // Act
                var users = this.database.GetPage<User>(
                    new PageIndexPageBuilder(3, 2),
                    "WHERE Name LIKE CONCAT(@Search, '%') and Age = @Age",
                    "Age DESC",
                    new { Search = "Some Name", Age = 10 }).Items;

                // Assert
                users.Should().BeEmpty();

                // Cleanup
                this.database.DeleteAll<User>();
            }

            [Fact]
            public void Returns_page_from_everything_when_conditions_is_null()
            {
                // Arrange
                this.database.Insert(new User { Name = "Some Name 1", Age = 10 });
                this.database.Insert(new User { Name = "Some Name 2", Age = 10 });
                this.database.Insert(new User { Name = "Some Name 3", Age = 10 });
                this.database.Insert(new User { Name = "Some Name 4", Age = 11 });

                // Act
                var users = this.database.GetPage<User>(new PageIndexPageBuilder(2, 2), null, "Age DESC").Items;

                // Assert
                users.Count().Should().Be(2);

                // Cleanup
                this.database.DeleteAll<User>();
            }

            [Collection(nameof(PostgresCollection))]
            public class Postgres
                : GetPage
            {
                public Postgres(PostgresFixture fixture)
                    : base(fixture)
                {
                }
            }

            [Collection(nameof(SqlServerCollection))]
            public class SqlServer
                : GetPage
            {
                public SqlServer(SqlServerFixture fixture)
                    : base(fixture)
                {
                }
            }
        }

        public abstract class GetPageWhereObject
            : DapperConnectionMicroCrudExtensionsTests
        {
            protected GetPageWhereObject(DatabaseFixture fixture)
                : base(fixture)
            {
            }

            [Fact]
            public void Returns_empty_list_when_there_are_no_entities()
            {
                // Act
                var pageBuilder = new PageIndexPageBuilder(1, 10);
                var users = this.database.GetPage<User>(pageBuilder, new { Age = 10 }, "Age");

                // Assert
                users.Items.Should().BeEmpty();
            }

            [Fact]
            public void Filters_result_by_conditions()
            {
                // Arrange
                this.database.Insert(new User { Name = "Some Name 1", Age = 10 });
                this.database.Insert(new User { Name = "Some Name 2", Age = 10 });
                this.database.Insert(new User { Name = "Some Name 3", Age = 10 });
                this.database.Insert(new User { Name = "Some Name 4", Age = 11 });

                // Act
                var pageBuilder = new PageIndexPageBuilder(1, 10);
                var users = this.database.GetPage<User>(pageBuilder, new { Age = 10 }, "Age");

                // Assert
                users.Items.Count().Should().Be(3);

                // Cleanup
                this.database.DeleteAll<User>();
            }

            [Fact]
            public void Gets_first_page()
            {
                // Arrange
                this.database.Insert(new User { Name = "Some Name 1", Age = 10 });
                this.database.Insert(new User { Name = "Some Name 2", Age = 10 });
                this.database.Insert(new User { Name = "Some Name 3", Age = 10 });
                this.database.Insert(new User { Name = "Some Name 4", Age = 11 });

                // Act
                var pageBuilder = new PageIndexPageBuilder(1, 2);
                var page = this.database.GetPage<User>(pageBuilder, new { Age = 10 }, "Age");
                var users = page.Items;

                // Assert
                users.Count().Should().Be(2);
                users[0].Name.Should().Be("Some Name 1");
                users[1].Name.Should().Be("Some Name 2");

                // Cleanup
                this.database.DeleteAll<User>();
            }

            [Fact]
            public void Gets_second_page()
            {
                // Arrange
                this.database.Insert(new User { Name = "Some Name 1", Age = 10 });
                this.database.Insert(new User { Name = "Some Name 2", Age = 10 });
                this.database.Insert(new User { Name = "Some Name 3", Age = 10 });
                this.database.Insert(new User { Name = "Some Name 4", Age = 11 });

                // Act
                var pageBuilder = new PageIndexPageBuilder(2, 2);
                var page = this.database.GetPage<User>(pageBuilder, new { Age = 10 }, "Age");
                var users = page.Items;

                // Assert
                users.Count().Should().Be(1);
                users[0].Name.Should().Be("Some Name 3");

                // Cleanup
                this.database.DeleteAll<User>();
            }

            [Fact]
            public void Returns_empty_set_past_last_page()
            {
                // Arrange
                this.database.Insert(new User { Name = "Some Name 1", Age = 10 });
                this.database.Insert(new User { Name = "Some Name 2", Age = 10 });
                this.database.Insert(new User { Name = "Some Name 3", Age = 10 });
                this.database.Insert(new User { Name = "Some Name 4", Age = 11 });

                // Act
                var pageBuilder = new PageIndexPageBuilder(3, 2);
                var page = this.database.GetPage<User>(pageBuilder, new { Age = 10 }, "Age");
                var users = page.Items;

                // Assert
                users.Should().BeEmpty();

                // Cleanup
                this.database.DeleteAll<User>();
            }

            [Collection(nameof(PostgresCollection))]
            public class Postgres
                : GetPageWhereObject
            {
                public Postgres(PostgresFixture fixture)
                    : base(fixture)
                {
                }
            }

            [Collection(nameof(SqlServerCollection))]
            public class SqlServer
                : GetPageWhereObject
            {
                public SqlServer(SqlServerFixture fixture)
                    : base(fixture)
                {
                }
            }
        }

        public abstract class GetAll
            : DapperConnectionMicroCrudExtensionsTests
        {
            protected GetAll(DatabaseFixture fixture)
                : base(fixture)
            {
            }

            [Fact]
            public void Gets_all()
            {
                // Arrange
                this.database.Insert(new User { Name = "Some Name 1", Age = 10 });
                this.database.Insert(new User { Name = "Some Name 2", Age = 10 });
                this.database.Insert(new User { Name = "Some Name 3", Age = 10 });
                this.database.Insert(new User { Name = "Some Name 4", Age = 11 });

                // Act
                var users = this.database.GetAll<User>();

                // Assert
                users.Count().Should().Be(4);

                // Cleanup
                this.database.DeleteAll<User>();
            }

            [Collection(nameof(PostgresCollection))]
            public class Postgres
                : GetAll
            {
                public Postgres(PostgresFixture fixture)
                    : base(fixture)
                {
                }
            }

            [Collection(nameof(SqlServerCollection))]
            public class SqlServer
                : GetAll
            {
                public SqlServer(SqlServerFixture fixture)
                    : base(fixture)
                {
                }
            }
        }

        public abstract class Insert
            : DapperConnectionMicroCrudExtensionsTests
        {
            protected Insert(DatabaseFixture fixture)
                : base(fixture)
            {
            }

            [Fact]
            public void Inserts_entity_with_int32_key()
            {
                // Arrange
                var entity = new KeyInt32 { Name = "Some Name" };

                // Act
                this.database.Insert(entity);

                // Assert
                this.database.Count<KeyInt32>().Should().Be(1);

                // Cleanup
                this.database.DeleteAll<KeyInt32>();
            }

            [Fact]
            public void Inserts_entity_with_int64_key()
            {
                // Arrange
                var entity = new KeyInt64 { Name = "Some Name" };

                // Act
                this.database.Insert(entity);

                // Assert
                this.database.Count<KeyInt64>().Should().Be(1);

                // Cleanup
                this.database.DeleteAll<KeyInt64>();
            }

            [Fact]
            public void Inserts_entities_with_composite_keys()
            {
                // Arrange
                var entity = new CompositeKeys { Key1 = 2, Key2 = 3, Name = "Some Name" };

                // Act
                this.database.Insert(entity);

                // Assert
                this.database.Count<CompositeKeys>().Should().Be(1);

                // Cleanup
                this.database.DeleteAll<CompositeKeys>();
            }

            [Fact]
            public void Does_not_allow_part_of_composite_key_to_be_null()
            {
                // Arrange
                var entity = new CompositeKeys { Key1 = null, Key2 = 5, Name = "Some Name" };

                // Act
                Action act = () => this.database.Insert(entity);

                // Assert
                act.ShouldThrow<Exception>();
            }

            [Fact]
            public void Inserts_entities_with_string_key()
            {
                // Arrange
                var entity = new KeyString { Name = "Some Name", Age = 10 };

                // Act
                this.database.Insert(entity);

                // Assert
                this.database.Count<KeyString>().Should().Be(1);

                // Cleanup
                this.database.DeleteAll<KeyString>();
            }

            [Fact]
            public void Does_not_allow_string_key_to_be_null()
            {
                // Arrange
                var entity = new KeyString { Name = null, Age = 10 };

                // Act
                Action act = () => this.database.Insert(entity);

                // Assert
                act.ShouldThrow<Exception>();
            }

            [Fact]
            public void Inserts_entities_with_guid_key()
            {
                // Arrange
                var entity = new KeyGuid { Id = Guid.NewGuid(), Name = "Some Name" };

                // Act
                this.database.Insert(entity);

                // Assert
                this.database.Count<KeyGuid>().Should().Be(1);

                // Cleanup
                this.database.DeleteAll<KeyGuid>();
            }

            [Fact]
            public void Uses_key_attribute_to_determine_key()
            {
                // Arrange
                var entity = new KeyAlias { Name = "Some Name" };

                // Act
                this.database.Insert(entity);

                // Assert
                this.database.Count<KeyAlias>().Should().Be(1);

                // Cleanup
                this.database.DeleteAll<KeyAlias>();
            }

            [Fact]
            public void Inserts_into_other_schemas()
            {
                // Arrange
                var entity = new SchemaOther { Name = "Some name" };

                // Act
                this.database.Insert(entity);

                // Assert
                this.database.Count<SchemaOther>().Should().Be(1);

                // Cleanup
                this.database.DeleteAll<SchemaOther>();
            }

            [Fact]
            public void Ignores_columns_which_are_not_mapped()
            {
                // Arrange
                var entity = new PropertyNotMapped { Firstname = "Bobby", LastName = "DropTables", Age = 10 };

                // Act
                this.database.Insert(entity);

                // Assert
                this.database.Count<PropertyNotMapped>().Should().Be(1);

                // Cleanup
                this.database.DeleteAll<PropertyNotMapped>();
            }

            [Collection(nameof(PostgresCollection))]
            public class Postgres
                : Insert
            {
                public Postgres(PostgresFixture fixture)
                    : base(fixture)
                {
                }
            }

            [Collection(nameof(SqlServerCollection))]
            public class SqlServer
                : Insert
            {
                public SqlServer(SqlServerFixture fixture)
                    : base(fixture)
                {
                }
            }
        }

        public abstract class InsertAndReturnKey
            : DapperConnectionMicroCrudExtensionsTests
        {
            protected InsertAndReturnKey(DatabaseFixture fixture)
                : base(fixture)
            {
            }

            [Fact]
            public void Throws_exception_when_entity_has_no_key()
            {
                // Act
                Assert.Throws<InvalidPrimaryKeyException>(
                    () => this.database.Insert<int>(new NoKey()));
            }

            [Fact]
            public void Throws_exception_when_entity_has_composite_keys()
            {
                // Act
                Assert.Throws<InvalidPrimaryKeyException>(
                    () => this.database.Insert<int>(new CompositeKeys()));
            }

            [Fact]
            public void Throws_exception_for_string_keys()
            {
                // Arrange
                var entity = new KeyString { Name = "Some Name", Age = 10 };

                // Act / Assert
                Assert.Throws<InvalidPrimaryKeyException>(
                    () => this.database.Insert<string>(entity));
            }

            [Fact]
            public void Throws_exception_for_guid_keys()
            {
                // Arrange
                var entity = new KeyGuid { Id = Guid.NewGuid(), Name = "Some Name" };

                // Act / Assert
                Assert.Throws<InvalidPrimaryKeyException>(
                    () => this.database.Insert<Guid>(entity));
            }

            [Fact]
            public void Inserts_entity_with_int32_primary_key()
            {
                // Act
                var id = this.database.Insert<int>(new KeyInt32 { Name = "Some Name" });

                // Assert
                id.Should().BeGreaterThan(0);

                // Cleanup
                this.database.Delete<KeyInt32>(id);
            }

            [Fact]
            public void Inserts_entity_with_int64_primary_key()
            {
                // Act
                var id = this.database.Insert<int>(new KeyInt64 { Name = "Some Name" });

                // Assert
                id.Should().BeGreaterThan(0);

                // Cleanup
                this.database.Delete<KeyInt64>(id);
            }

            [Fact]
            public void Uses_key_attribute_to_determine_key()
            {
                // Act
                var id = this.database.Insert<int>(new KeyAlias { Name = "Some Name" });

                // Assert
                id.Should().BeGreaterThan(0);

                // Cleanup
                this.database.Delete<KeyAlias>(id);
            }

            [Fact]
            public void Inserts_into_other_schemas()
            {
                // Act
                var id = this.database.Insert<int>(new SchemaOther { Name = "Some name" });

                // Assert
                id.Should().BeGreaterThan(0);

                // Cleanup
                this.database.Delete<SchemaOther>(id);
            }

            [Collection(nameof(PostgresCollection))]
            public class Postgres
                : InsertAndReturnKey
            {
                public Postgres(PostgresFixture fixture)
                    : base(fixture)
                {
                }
            }

            [Collection(nameof(SqlServerCollection))]
            public class SqlServer
                : InsertAndReturnKey
            {
                public SqlServer(SqlServerFixture fixture)
                    : base(fixture)
                {
                }
            }
        }

        public abstract class InsertRange
            : DapperConnectionMicroCrudExtensionsTests
        {
            protected InsertRange(DatabaseFixture fixture)
                : base(fixture)
            {
            }

            [Fact]
            public void Inserts_entity_with_int32_key()
            {
                // Arrange
                var entities = new[]
                    {
                        new KeyInt32 { Name = "Some Name" },
                        new KeyInt32 { Name = "Some Name2" }
                    };

                // Act
                this.database.InsertRange(entities);

                // Assert
                this.database.Count<KeyInt32>().Should().Be(2);

                // Cleanup
                this.database.DeleteAll<KeyInt32>();
            }

            [Fact]
            public void Inserts_entity_with_int64_key()
            {
                // Arrange
                var entities = new[]
                    {
                        new KeyInt64 { Name = "Some Name" },
                        new KeyInt64 { Name = "Some Name2" }
                    };

                // Act
                this.database.InsertRange(entities);

                // Assert
                this.database.Count<KeyInt64>().Should().Be(2);

                // Cleanup
                this.database.DeleteAll<KeyInt64>();
            }

            [Fact]
            public void Inserts_entities_with_composite_keys()
            {
                // Arrange
                var entities = new[]
                    {
                        new CompositeKeys { Key1 = 2, Key2 = 3, Name = "Some Name1" },
                        new CompositeKeys { Key1 = 3, Key2 = 3, Name = "Some Name2" }
                    };

                // Act
                this.database.InsertRange(entities);

                // Assert
                this.database.Count<CompositeKeys>().Should().Be(2);

                // Cleanup
                this.database.DeleteAll<CompositeKeys>();
            }

            [Fact]
            public void Inserts_entities_with_string_key()
            {
                // Arrange
                var entities = new[]
                    {
                        new KeyString { Name = "Some Name", Age = 10 },
                        new KeyString { Name = "Some Name2", Age = 11 }
                    };

                // Act
                this.database.InsertRange(entities);

                // Assert
                this.database.Count<KeyString>().Should().Be(2);

                // Cleanup
                this.database.DeleteAll<KeyString>();
            }

            [Fact]
            public void Inserts_entities_with_guid_key()
            {
                // Arrange
                var entities = new[]
                    {
                        new KeyGuid { Id = Guid.NewGuid(), Name = "Some Name" },
                        new KeyGuid { Id = Guid.NewGuid(), Name = "Some Name2" }
                    };

                // Act
                this.database.InsertRange(entities);

                // Assert
                this.database.Count<KeyGuid>().Should().Be(2);

                // Cleanup
                this.database.DeleteAll<KeyGuid>();
            }

            [Fact]
            public void Uses_key_attribute_to_determine_key()
            {
                // Arrange
                var entities = new[]
                    {
                        new KeyAlias { Name = "Some Name" },
                        new KeyAlias { Name = "Some Name2" }
                    };

                // Act
                this.database.InsertRange(entities);

                // Assert
                this.database.Count<KeyAlias>().Should().Be(2);

                // Cleanup
                this.database.DeleteAll<KeyAlias>();
            }

            [Fact]
            public void Inserts_into_other_schemas()
            {
                // Arrange
                var entities = new[]
                    {
                        new SchemaOther { Name = "Some Name" },
                        new SchemaOther { Name = "Some Name2" }
                    };

                // Act
                this.database.InsertRange(entities);

                // Assert
                this.database.Count<SchemaOther>().Should().Be(2);

                // Cleanup
                this.database.DeleteAll<SchemaOther>();
            }

            [Collection(nameof(PostgresCollection))]
            public class Postgres
                : InsertRange
            {
                public Postgres(PostgresFixture fixture)
                    : base(fixture)
                {
                }
            }

            [Collection(nameof(SqlServerCollection))]
            public class SqlServer
                : InsertRange
            {
                public SqlServer(SqlServerFixture fixture)
                    : base(fixture)
                {
                }
            }
        }

        public abstract class InsertRangeAndSetKey
            : DapperConnectionMicroCrudExtensionsTests
        {
            protected InsertRangeAndSetKey(DatabaseFixture fixture)
                : base(fixture)
            {
            }

            [Fact]
            public void Throws_exception_when_entity_has_no_key()
            {
                // Arrange
                var entities = new[]
                    {
                        new NoKey()
                    };

                // Act / Assert
                Assert.Throws<InvalidPrimaryKeyException>(
                    () => this.database.InsertRange<NoKey, int>(entities, (e, k) => { }));
            }

            [Fact]
            public void Throws_exception_when_entity_has_composite_keys()
            {
                // Arrange
                var entities = new[]
                    {
                        new CompositeKeys()
                    };

                // Act / Assert
                Assert.Throws<InvalidPrimaryKeyException>(
                    () => this.database.InsertRange<CompositeKeys, int>(entities, (e, k) => { }));
            }

            [Fact]
            public void Throws_exception_for_string_keys()
            {
                // Arrange
                var entities = new[]
                    {
                        new KeyString { Name = "Some Name", Age = 10 }
                    };

            // Act / Assert
            Assert.Throws<InvalidPrimaryKeyException>(
                    () => this.database.InsertRange<KeyString, string>(entities, (e, k) => { }));
            }

            [Fact]
            public void Throws_exception_for_guid_keys()
            {
                // Arrange
                var entities = new[]
                    {
                        new KeyGuid { Id = Guid.NewGuid(), Name = "Some Name" }
                    };

                // Act / Assert
                Assert.Throws<InvalidPrimaryKeyException>(
                        () => this.database.InsertRange<KeyGuid, Guid>(entities, (e, k) => { }));
            }

            [Fact]
            public void Inserts_entity_with_int32_primary_key()
            {
                // Arrange
                var entities = new[]
                    {
                        new KeyInt32 { Name = "Some Name" },
                        new KeyInt32 { Name = "Some Name2" },
                        new KeyInt32 { Name = "Some Name3" }
                    };

                // Act
                this.database.InsertRange<KeyInt32, int>(entities, (e, k) => { e.Id = k; });

                // Assert
                entities[0].Id.Should().BeGreaterThan(0);
                entities[1].Id.Should().BeGreaterThan(entities[0].Id);
                entities[2].Id.Should().BeGreaterThan(entities[1].Id);

                // Cleanup
                this.database.DeleteAll<KeyInt32>();
            }

            [Fact]
            public void Inserts_entity_with_int64_primary_key()
            {
                // Arrange
                var entities = new[]
                    {
                        new KeyInt64 { Name = "Some Name" },
                        new KeyInt64 { Name = "Some Name2" },
                        new KeyInt64 { Name = "Some Name3" }
                    };

                // Act
                this.database.InsertRange<KeyInt64, long>(entities, (e, k) => { e.Id = k; });

                // Assert
                entities[0].Id.Should().BeGreaterThan(0);
                entities[1].Id.Should().BeGreaterThan(entities[0].Id);
                entities[2].Id.Should().BeGreaterThan(entities[1].Id);

                // Cleanup
                this.database.DeleteAll<KeyInt64>();
            }

            [Fact]
            public void Uses_key_attribute_to_determine_key()
            {
                // Arrange
                var entities = new[]
                    {
                        new KeyExplicit { Name = "Some Name" }
                    };

                // Act
                this.database.InsertRange<KeyExplicit, int>(entities, (e, k) => { e.Key = k; });

                // Assert
                entities[0].Key.Should().BeGreaterThan(0);

                // Cleanup
                this.database.DeleteAll<KeyAlias>();
            }

            [Fact]
            public void Inserts_into_other_schemas()
            {
                // Arrange
                var entities = new[]
                    {
                        new SchemaOther { Name = "Some Name" },
                        new SchemaOther { Name = "Some Name2" },
                        new SchemaOther { Name = "Some Name3" }
                    };

                // Act
                this.database.InsertRange<SchemaOther, int>(entities, (e, k) => { e.Id = k; });

                // Assert
                entities[0].Id.Should().BeGreaterThan(0);
                entities[1].Id.Should().BeGreaterThan(entities[0].Id);
                entities[2].Id.Should().BeGreaterThan(entities[1].Id);

                // Cleanup
                this.database.DeleteAll<SchemaOther>();
            }

            [Collection(nameof(PostgresCollection))]
            public class Postgres
                : InsertRangeAndSetKey
            {
                public Postgres(PostgresFixture fixture)
                    : base(fixture)
                {
                }
            }

            [Collection(nameof(SqlServerCollection))]
            public class SqlServer
                : InsertRangeAndSetKey
            {
                public SqlServer(SqlServerFixture fixture)
                    : base(fixture)
                {
                }
            }
        }

        public abstract class Update
            : DapperConnectionMicroCrudExtensionsTests
        {
            protected Update(DatabaseFixture fixture)
                : base(fixture)
            {
            }

            [Fact]
            public void Updates_the_entity()
            {
                // Arrange
                var id = this.database.Insert<int>(new User { Name = "Some name", Age = 10 });

                // Act
                var entity = this.database.Find<User>(id);
                entity.Name = "Other name";
                this.database.Update(entity);

                // Assert
                var updatedEntity = this.database.Find<User>(id);
                updatedEntity.Name.Should().Be("Other name");

                // Cleanup
                this.database.Delete<User>(id);
            }

            [Fact]
            public void Ignores_columns_which_are_not_mapped()
            {
                // Arrange
                var entity = new PropertyNotMapped { Firstname = "Bobby", LastName = "DropTables", Age = 10 };
                entity.Id = this.database.Insert<int>(entity);

                // Act
                entity.LastName = "Other name";
                this.database.Update(entity);

                // Assert
                var updatedEntity = this.database.Find<PropertyNotMapped>(entity.Id);
                updatedEntity.LastName.Should().Be("Other name");

                // Cleanup
                this.database.DeleteAll<PropertyNotMapped>();
            }

            [Fact]
            public void Updates_entities_with_composite_keys()
            {
                // Arrange
                var entity = new CompositeKeys { Key1 = 5, Key2 = 20, Name = "Some name" };
                this.database.Insert(entity);

                // Act
                entity.Name = "Other name";
                this.database.Update(entity);

                // Assert
                var id = new { Key1 = 5, Key2 = 20 };
                var updatedEntity = this.database.Find<CompositeKeys>(id);

                updatedEntity.Name.Should().Be("Other name");

                // Cleanup
                this.database.DeleteAll<CompositeKeys>();
            }

            [Collection(nameof(PostgresCollection))]
            public class Postgres
                : Update
            {
                public Postgres(PostgresFixture fixture)
                    : base(fixture)
                {
                }
            }

            [Collection(nameof(SqlServerCollection))]
            public class SqlServer
                : Update
            {
                public SqlServer(SqlServerFixture fixture)
                    : base(fixture)
                {
                }
            }
        }

        public abstract class UpdateRange
            : DapperConnectionMicroCrudExtensionsTests
        {
            protected UpdateRange(DatabaseFixture fixture)
                : base(fixture)
            {
            }

            [Fact]
            public void Updates_the_entity()
            {
                // Arrange
                this.database.InsertRange(
                    new[]
                        {
                            new User { Name = "Some name1", Age = 10 },
                            new User { Name = "Some name2", Age = 10 },
                            new User { Name = "Some name2", Age = 11 }
                        }
                    );

                // Act
                var entities = this.database.GetRange<User>("WHERE Age = 10").ToList();
                foreach (var entity in entities)
                {
                    entity.Name = "Other name";
                }

                var result = this.database.UpdateRange(entities);

                // Assert
                result.NumRowsAffected.Should().Be(2);

                var updatedEntities = this.database.GetRange<User>("WHERE Name = 'Other name'");
                updatedEntities.Count().Should().Be(2);

                // Cleanup
                this.database.DeleteAll<User>();
            }

            [Fact]
            public void Updates_entities_with_composite_keys()
            {
                // Arrange
                this.database.InsertRange(
                    new[]
                        {
                            new CompositeKeys { Key1 = 5, Key2 = 20, Name = "Some name1" },
                            new CompositeKeys { Key1 = 6, Key2 = 21, Name = "Some name2" },
                            new CompositeKeys { Key1 = 7, Key2 = 22, Name = "Some other name" }
                        }
                    );

                // Act
                var entities = this.database.GetRange<CompositeKeys>("WHERE Name Like 'Some name%'").ToList();

                foreach (var entity in entities)
                {
                    entity.Name = "Other name";
                }

                var result = this.database.UpdateRange(entities);

                // Assert
                result.NumRowsAffected.Should().Be(2);

                var updatedEntities = this.database.GetRange<CompositeKeys>("WHERE Name = 'Other name'");
                updatedEntities.Count().Should().Be(2);

                // Cleanup
                this.database.DeleteAll<CompositeKeys>();
            }

            [Collection(nameof(PostgresCollection))]
            public class Postgres
                : UpdateRange
            {
                public Postgres(PostgresFixture fixture)
                    : base(fixture)
                {
                }
            }

            [Collection(nameof(SqlServerCollection))]
            public class SqlServer
                : UpdateRange
            {
                public SqlServer(SqlServerFixture fixture)
                    : base(fixture)
                {
                }
            }
        }

        public abstract class DeleteId
            : DapperConnectionMicroCrudExtensionsTests
        {
            protected DeleteId(DatabaseFixture fixture)
                : base(fixture)
            {
            }

            [Fact]
            public void Deletes_the_entity_with_the_specified_id()
            {
                // Arrange
                var id = this.database.Insert<int>(new User { Name = "Some name", Age = 10 });

                // Act
                this.database.Delete<User>(id);

                // Assert
                this.database.Find<User>(id).Should().BeNull();
            }

            [Fact]
            public void Deletes_entity_with_string_key()
            {
                // Arrange
                this.database.Insert(new KeyString { Name = "Some Name", Age = 10 });

                // Act
                this.database.Delete<KeyString>("Some Name");
            }

            [Fact]
            public void Deletes_entity_with_guid_key()
            {
                // Arrange
                var id = Guid.NewGuid();
                this.database.Insert(new KeyGuid { Id = id, Name = "Some Name" });

                // Act
                this.database.Delete<KeyGuid>(id);
            }

            [Fact]
            public void Deletes_entity_with_composite_keys()
            {
                // Arrange
                var id = new { Key1 = 5, Key2 = 20 };
                var entity = new CompositeKeys { Key1 = 5, Key2 = 20, Name = "Some Name" };
                this.database.Insert(entity);

                // Act
                this.database.Delete<CompositeKeys>(id);
            }

            [Collection(nameof(PostgresCollection))]
            public class Postgres
                : DeleteId
            {
                public Postgres(PostgresFixture fixture)
                    : base(fixture)
                {
                }
            }

            [Collection(nameof(SqlServerCollection))]
            public class SqlServer
                : DeleteId
            {
                public SqlServer(SqlServerFixture fixture)
                    : base(fixture)
                {
                }
            }
        }

        public abstract class DeleteEntity
            : DapperConnectionMicroCrudExtensionsTests
        {
            protected DeleteEntity(DatabaseFixture fixture)
                : base(fixture)
            {
            }

            [Fact]
            public void Deletes_entity_with_matching_key()
            {
                // Arrange
                var id = this.database.Insert<int>(new User { Name = "Some name", Age = 10 });

                // Act
                var entity = this.database.Find<User>(id);
                this.database.Delete(entity);

                // Assert
                this.database.Find<User>(id).Should().BeNull();
            }

            [Fact]
            public void Deletes_entity_with_composite_keys()
            {
                // Arrange
                var id = new { Key1 = 5, Key2 = 20 };
                var entity = new CompositeKeys { Key1 = 5, Key2 = 20, Name = "Some Name" };
                this.database.Insert(entity);

                // Act
                this.database.Delete(entity);

                // Assert
                this.database.Find<CompositeKeys>(id).Should().BeNull();
            }

            [Collection(nameof(PostgresCollection))]
            public class Postgres
                : DeleteEntity
            {
                public Postgres(PostgresFixture fixture)
                    : base(fixture)
                {
                }
            }

            [Collection(nameof(SqlServerCollection))]
            public class SqlServer
                : DeleteEntity
            {
                public SqlServer(SqlServerFixture fixture)
                    : base(fixture)
                {
                }
            }
        }

        public abstract class DeleteRange
            : DapperConnectionMicroCrudExtensionsTests
        {
            protected DeleteRange(DatabaseFixture fixture)
                : base(fixture)
            {
            }

            [Theory]
            [InlineData(null)]
            [InlineData("")]
            [InlineData(" ")]
            [InlineData("HAVING Age = 10")]
            [InlineData("WHERE")]
            public void Throws_exception_if_conditions_does_not_contain_where_clause(string conditions)
            {
                // Act / Assert
                Assert.Throws<ArgumentException>(
                    () => this.database.DeleteRange<User>(conditions));
            }

            [Theory]
            [InlineData("Where Age = 10")]
            [InlineData("where Age = 10")]
            [InlineData("WHERE Age = 10")]
            public void Allows_any_capitalization_of_where_clause(string conditions)
            {
                // Act
                Action act = () => this.database.DeleteRange<User>(conditions);

                // Assert
                act.ShouldNotThrow();
            }

            [Fact]
            public void Deletes_all_matching_entities()
            {
                // Arrange
                this.database.Insert(new User { Name = "Some Name 1", Age = 10 });
                this.database.Insert(new User { Name = "Some Name 2", Age = 10 });
                this.database.Insert(new User { Name = "Some Name 3", Age = 10 });
                this.database.Insert(new User { Name = "Some Name 4", Age = 11 });

                // Act
                var result = this.database.DeleteRange<User>("WHERE Age = @Age", new { Age = 10 });

                // Assert
                result.NumRowsAffected.Should().Be(3);
                this.database.Count<User>().Should().Be(1);

                // Cleanup
                this.database.DeleteAll<User>();
            }

            [Collection(nameof(PostgresCollection))]
            public class Postgres
                : DeleteRange
            {
                public Postgres(PostgresFixture fixture)
                    : base(fixture)
                {
                }
            }

            [Collection(nameof(SqlServerCollection))]
            public class SqlServer
                : DeleteRange
            {
                public SqlServer(SqlServerFixture fixture)
                    : base(fixture)
                {
                }
            }
        }

        public abstract class DeleteRangeWhereObject
            : DapperConnectionMicroCrudExtensionsTests
        {
            protected DeleteRangeWhereObject(DatabaseFixture fixture)
                : base(fixture)
            {
            }

            [Fact]
            public void Throws_exception_if_conditions_is_null()
            {
                // Act / Assert
                Assert.Throws<ArgumentNullException>(() => this.database.DeleteRange<User>((object)null));
            }

            [Fact]
            public void Throws_exception_if_conditions_is_empty()
            {
                // Act / Assert
                Assert.Throws<ArgumentException>(() => this.database.DeleteRange<User>(new { }));
            }

            [Fact]
            public void Deletes_all_matching_entities()
            {
                // Arrange
                this.database.Insert(new User { Name = "Some Name 1", Age = 10 });
                this.database.Insert(new User { Name = "Some Name 2", Age = 10 });
                this.database.Insert(new User { Name = "Some Name 3", Age = 10 });
                this.database.Insert(new User { Name = "Some Name 4", Age = 11 });

                // Act
                var result = this.database.DeleteRange<User>(new { Age = 10 });

                // Assert
                result.NumRowsAffected.Should().Be(3);
                this.database.Count<User>().Should().Be(1);

                // Cleanup
                this.database.DeleteAll<User>();
            }

            [Collection(nameof(PostgresCollection))]
            public class Postgres
                : DeleteRangeWhereObject
            {
                public Postgres(PostgresFixture fixture)
                    : base(fixture)
                {
                }
            }

            [Collection(nameof(SqlServerCollection))]
            public class SqlServer
                : DeleteRangeWhereObject
            {
                public SqlServer(SqlServerFixture fixture)
                    : base(fixture)
                {
                }
            }
        }

        public abstract class DeleteAll
            : DapperConnectionMicroCrudExtensionsTests
        {
            protected DeleteAll(DatabaseFixture fixture)
                : base(fixture)
            {
            }

            [Fact]
            public void Deletes_all_entities()
            {
                // Arrange
                this.database.Insert(new User { Name = "Some Name 1", Age = 10 });
                this.database.Insert(new User { Name = "Some Name 2", Age = 10 });
                this.database.Insert(new User { Name = "Some Name 3", Age = 10 });
                this.database.Insert(new User { Name = "Some Name 4", Age = 11 });

                // Act
                var result = this.database.DeleteAll<User>();

                // Assert
                result.NumRowsAffected.Should().Be(4);
                this.database.Count<User>().Should().Be(0);
            }

            [Collection(nameof(PostgresCollection))]
            public class Postgres
                : DeleteAll
            {
                public Postgres(PostgresFixture fixture)
                    : base(fixture)
                {
                }
            }

            [Collection(nameof(SqlServerCollection))]
            public class SqlServer
                : DeleteAll
            {
                public SqlServer(SqlServerFixture fixture)
                    : base(fixture)
                {
                }
            }
        }
    }
}