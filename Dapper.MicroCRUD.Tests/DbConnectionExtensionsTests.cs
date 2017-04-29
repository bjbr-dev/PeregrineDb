// <copyright file="DbConnectionExtensionsTests.cs" company="Berkeleybross">
//   Copyright (c) Berkeleybross. All rights reserved.
// </copyright>
namespace Dapper.MicroCRUD.Tests
{
    using System;
    using System.Data;
    using System.Linq;
    using Dapper.MicroCRUD.Dialects;
    using Dapper.MicroCRUD.Schema;
    using Dapper.MicroCRUD.Tests.Dialects.Postgres;
    using Dapper.MicroCRUD.Tests.Dialects.SqlServer;
    using Dapper.MicroCRUD.Tests.ExampleEntities;
    using FluentAssertions;
    using Pagination;
    using Xunit;

    public abstract class DbConnectionExtensionsTests
    {
        private readonly IDbConnection connection;
        private readonly IDialect dialect;

        protected DbConnectionExtensionsTests(DatabaseFixture fixture)
        {
            this.dialect = fixture?.DatabaseDialect;
            this.connection = fixture?.Database?.Connection;
        }

        public class Misc
            : DbConnectionExtensionsTests
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
                var sutType = typeof(DbConnectionExtensions);

                sutType.Namespace.Should().Be(dapperType.Namespace);
            }
        }

        public abstract class Count
            : DbConnectionExtensionsTests
        {
            public Count(DatabaseFixture fixture)
                : base(fixture)
            {
            }

            [Fact]
            public void Counts_all_entities_when_conditions_is_null()
            {
                // Arrange
                this.connection.Insert(new User { Name = "Some Name 1", Age = 10 }, dialect: this.dialect);
                this.connection.Insert(new User { Name = "Some Name 2", Age = 10 }, dialect: this.dialect);
                this.connection.Insert(new User { Name = "Some Name 3", Age = 10 }, dialect: this.dialect);
                this.connection.Insert(new User { Name = "Some Name 4", Age = 11 }, dialect: this.dialect);

                // Act
                var result = this.connection.Count<User>(dialect: this.dialect);

                // Assert
                result.Should().Be(4);

                // Cleanup
                this.connection.DeleteAll<User>(dialect: this.dialect);
            }

            [Fact]
            public void Counts_entities_matching_conditions()
            {
                // Arrange
                this.connection.Insert(new User { Name = "Some Name 1", Age = 10 }, dialect: this.dialect);
                this.connection.Insert(new User { Name = "Some Name 2", Age = 10 }, dialect: this.dialect);
                this.connection.Insert(new User { Name = "Some Name 3", Age = 10 }, dialect: this.dialect);
                this.connection.Insert(new User { Name = "Some Name 4", Age = 11 }, dialect: this.dialect);

                // Act
                var result = this.connection.Count<User>("WHERE Age < @Age", new { Age = 11 }, dialect: this.dialect);

                // Assert
                result.Should().Be(3);

                // Cleanup
                this.connection.DeleteAll<User>(dialect: this.dialect);
            }

            [Fact]
            public void Counts_entities_in_alternate_schema()
            {
                this.connection.Insert<int>(new SchemaOther { Name = "Some Name" }, dialect: this.dialect);
                this.connection.Insert<int>(new SchemaOther { Name = "Some Name" }, dialect: this.dialect);
                this.connection.Insert<int>(new SchemaOther { Name = "Some Name" }, dialect: this.dialect);
                this.connection.Insert<int>(new SchemaOther { Name = "Some Name" }, dialect: this.dialect);

                // Act
                var result = this.connection.Count<SchemaOther>(dialect: this.dialect);

                // Assert
                result.Should().Be(4);

                // Cleanup
                this.connection.DeleteAll<SchemaOther>(dialect: this.dialect);
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
            : DbConnectionExtensionsTests
        {
            public CountWhereObject(DatabaseFixture fixture)
                : base(fixture)
            {
            }

            [Fact]
            public void Throws_exception_when_conditions_is_null()
            {
                // Act
                Assert.Throws<ArgumentNullException>(() => this.connection.Count<User>((object)null, dialect: this.dialect));
            }

            [Fact]
            public void Counts_all_entities_when_conditions_is_empty()
            {
                // Arrange
                this.connection.Insert(new User { Name = "Some Name 1", Age = 10 }, dialect: this.dialect);
                this.connection.Insert(new User { Name = "Some Name 2", Age = 10 }, dialect: this.dialect);
                this.connection.Insert(new User { Name = "Some Name 3", Age = 10 }, dialect: this.dialect);
                this.connection.Insert(new User { Name = "Some Name 4", Age = 11 }, dialect: this.dialect);

                // Act
                var result = this.connection.Count<User>(new { }, dialect: this.dialect);

                // Assert
                result.Should().Be(4);

                // Cleanup
                this.connection.DeleteAll<User>(dialect: this.dialect);
            }

            [Fact]
            public void Counts_entities_matching_conditions()
            {
                // Arrange
                this.connection.Insert(new User { Name = "Some Name 1", Age = 10 }, dialect: this.dialect);
                this.connection.Insert(new User { Name = "Some Name 2", Age = 10 }, dialect: this.dialect);
                this.connection.Insert(new User { Name = "Some Name 3", Age = 10 }, dialect: this.dialect);
                this.connection.Insert(new User { Name = "Some Name 4", Age = 11 }, dialect: this.dialect);

                // Act
                var result = this.connection.Count<User>(new { Age = 10 }, dialect: this.dialect);

                // Assert
                result.Should().Be(3);

                // Cleanup
                this.connection.DeleteAll<User>(dialect: this.dialect);
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
            : DbConnectionExtensionsTests
        {
            public Find(DatabaseFixture fixture)
                : base(fixture)
            {
            }

            [Fact]
            public void Throws_exception_when_entity_has_no_key()
            {
                // Arrange
                this.connection.Insert(new NoKey { Name = "Some Name", Age = 1 }, dialect: this.dialect);

                // Act
                Assert.Throws<InvalidPrimaryKeyException>(
                    () => this.connection.Find<NoKey>("Some Name", dialect: this.dialect));
            }

            [Fact]
            public void Returns_null_when_entity_is_not_found()
            {
                // Act
                var entity = this.connection.Find<KeyInt32>(12, dialect: this.dialect);

                // Assert
                entity.Should().Be(null);
            }

            [Fact]
            public void Finds_entity_by_Int32_primary_key()
            {
                // Arrange
                var id = this.connection.Insert<int>(new KeyInt32 { Name = "Some Name" }, dialect: this.dialect);

                // Act
                var entity = this.connection.Find<KeyInt32>(id, dialect: this.dialect);

                // Assert
                entity.Name.Should().Be("Some Name");

                // Cleanup
                this.connection.Delete<KeyInt32>(id, dialect: this.dialect);
            }

            [Fact]
            public void Finds_entity_by_Int64_primary_key()
            {
                // Arrange
                var id = this.connection.Insert<long>(new KeyInt64 { Name = "Some Name" }, dialect: this.dialect);

                // Act
                var user = this.connection.Find<KeyInt64>(id, dialect: this.dialect);

                // Assert
                user.Name.Should().Be("Some Name");

                // Cleanup
                this.connection.Delete<KeyInt64>(id, dialect: this.dialect);
            }

            [Fact]
            public void Finds_entity_by_string_primary_key()
            {
                // Arrange
                this.connection.Insert(new KeyString { Name = "Some Name", Age = 42 }, dialect: this.dialect);

                // Act
                var entity = this.connection.Find<KeyString>("Some Name", dialect: this.dialect);

                // Assert
                entity.Age.Should().Be(42);

                // Cleanup
                this.connection.Delete(entity, dialect: this.dialect);
            }

            [Fact]
            public void Finds_entity_by_guid_primary_key()
            {
                // Arrange
                var id = Guid.NewGuid();
                this.connection.Insert(new KeyGuid { Id = id, Name = "Some Name" }, dialect: this.dialect);

                // Act
                var entity = this.connection.Find<KeyGuid>(id, dialect: this.dialect);

                // Assert
                entity.Name.Should().Be("Some Name");

                // Cleanup
                this.connection.Delete(entity, dialect: this.dialect);
            }

            [Fact]
            public void Finds_entity_by_composite_key()
            {
                // Arrange
                this.connection.Insert(new CompositeKeys { Key1 = 1, Key2 = 1, Name = "Some Name" }, dialect: this.dialect);
                var id = new { Key1 = 1, Key2 = 1 };

                // Act
                var entity = this.connection.Find<CompositeKeys>(id, dialect: this.dialect);

                // Assert
                entity.Name.Should().Be("Some Name");

                // Cleanup
                this.connection.DeleteAll<CompositeKeys>(dialect: this.dialect);
            }

            [Fact]
            public void Finds_entities_in_alternate_schema()
            {
                // Arrange
                var id = this.connection.Insert<int>(new SchemaOther { Name = "Some Name" }, dialect: this.dialect);

                // Act
                var entity = this.connection.Find<SchemaOther>(id, dialect: this.dialect);

                // Assert
                entity.Name.Should().Be("Some Name");

                // Cleanup
                this.connection.Delete<SchemaOther>(id, dialect: this.dialect);
            }

            [Fact]
            public void Finds_entities_with_enum_property()
            {
                // Arrange
                var id = this.connection.Insert<int>(
                    new PropertyEnum { FavoriteColor = Color.Green },
                    dialect: this.dialect);

                // Act
                var entity = this.connection.Find<PropertyEnum>(id, dialect: this.dialect);

                // Assert
                entity.FavoriteColor.Should().Be(Color.Green);

                // Cleanup
                this.connection.Delete<PropertyEnum>(id, dialect: this.dialect);
            }

            [Fact]
            public void Finds_entities_with_all_possible_types()
            {
                // Arrange
                var id = this.connection.Insert<int>(
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
                        },
                    dialect: this.dialect);

                // Act
                var entity = this.connection.Find<PropertyAllPossibleTypes>(id, dialect: this.dialect);

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
                this.connection.Delete<PropertyAllPossibleTypes>(id, dialect: this.dialect);
            }

            [Fact]
            public void Ignores_columns_which_are_not_mapped()
            {
                // Arrange
                var id = this.connection.Insert<int>(
                    new PropertyNotMapped { Firstname = "Bobby", LastName = "DropTables", Age = 10 },
                    dialect: this.dialect);

                // Act
                var entity = this.connection.Find<PropertyNotMapped>(id, dialect: this.dialect);

                // Assert
                entity.Firstname.Should().Be("Bobby");
                entity.LastName.Should().Be("DropTables");
                entity.FullName.Should().Be("Bobby DropTables");
                entity.Age.Should().Be(0);

                // Cleanup
                this.connection.DeleteAll<PropertyNotMapped>(dialect: this.dialect);
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
            : DbConnectionExtensionsTests
        {
            public Get(DatabaseFixture fixture)
                : base(fixture)
            {
            }

            [Fact]
            public void Throws_exception_when_entity_has_no_key()
            {
                // Arrange
                this.connection.Insert(new NoKey { Name = "Some Name", Age = 1 }, dialect: this.dialect);

                // Act
                Assert.Throws<InvalidPrimaryKeyException>(
                    () => this.connection.Get<NoKey>("Some Name", dialect: this.dialect));
            }

            [Fact]
            public void Throws_exception_when_entity_is_not_found()
            {
                // Act
                Assert.Throws<InvalidOperationException>(
                    () => this.connection.Get<KeyInt32>(5, dialect: this.dialect));
            }

            [Fact]
            public void Finds_entity_by_Int32_primary_key()
            {
                // Arrange
                var id = this.connection.Insert<int>(new KeyInt32 { Name = "Some Name" }, dialect: this.dialect);

                // Act
                var entity = this.connection.Get<KeyInt32>(id, dialect: this.dialect);

                // Assert
                entity.Name.Should().Be("Some Name");

                // Cleanup
                this.connection.Delete<KeyInt32>(id, dialect: this.dialect);
            }

            [Fact]
            public void Finds_entity_by_Int64_primary_key()
            {
                // Arrange
                var id = this.connection.Insert<long>(new KeyInt64 { Name = "Some Name" }, dialect: this.dialect);

                // Act
                var user = this.connection.Get<KeyInt64>(id, dialect: this.dialect);

                // Assert
                user.Name.Should().Be("Some Name");

                // Cleanup
                this.connection.Delete<KeyInt64>(id, dialect: this.dialect);
            }

            [Fact]
            public void Finds_entity_by_string_primary_key()
            {
                // Arrange
                this.connection.Insert(new KeyString { Name = "Some Name", Age = 42 }, dialect: this.dialect);

                // Act
                var entity = this.connection.Get<KeyString>("Some Name", dialect: this.dialect);

                // Assert
                entity.Age.Should().Be(42);

                // Cleanup
                this.connection.Delete(entity, dialect: this.dialect);
            }

            [Fact]
            public void Finds_entity_by_guid_primary_key()
            {
                // Arrange
                var id = Guid.NewGuid();
                this.connection.Insert(new KeyGuid { Id = id, Name = "Some Name" }, dialect: this.dialect);

                // Act
                var entity = this.connection.Get<KeyGuid>(id, dialect: this.dialect);

                // Assert
                entity.Name.Should().Be("Some Name");

                // Cleanup
                this.connection.Delete(entity, dialect: this.dialect);
            }

            [Fact]
            public void Finds_entity_by_composite_key()
            {
                // Arrange
                this.connection.Insert(new CompositeKeys { Key1 = 1, Key2 = 1, Name = "Some Name" }, dialect: this.dialect);
                var id = new { Key1 = 1, Key2 = 1 };

                // Act
                var entity = this.connection.Get<CompositeKeys>(id, dialect: this.dialect);

                // Assert
                entity.Name.Should().Be("Some Name");

                // Cleanup
                this.connection.DeleteAll<CompositeKeys>(dialect: this.dialect);
            }

            [Fact]
            public void Finds_entities_in_alternate_schema()
            {
                // Arrange
                var id = this.connection.Insert<int>(new SchemaOther { Name = "Some Name" }, dialect: this.dialect);

                // Act
                var entity = this.connection.Get<SchemaOther>(id, dialect: this.dialect);

                // Assert
                entity.Name.Should().Be("Some Name");

                // Cleanup
                this.connection.Delete<SchemaOther>(id, dialect: this.dialect);
            }

            [Fact]
            public void Ignores_columns_which_are_not_mapped()
            {
                // Arrange
                var id = this.connection.Insert<int>(
                    new PropertyNotMapped { Firstname = "Bobby", LastName = "DropTables", Age = 10 },
                    dialect: this.dialect);

                // Act
                var entity = this.connection.Get<PropertyNotMapped>(id, dialect: this.dialect);

                // Assert
                entity.Firstname.Should().Be("Bobby");
                entity.LastName.Should().Be("DropTables");
                entity.FullName.Should().Be("Bobby DropTables");
                entity.Age.Should().Be(0);

                // Cleanup
                this.connection.DeleteAll<PropertyNotMapped>(dialect: this.dialect);
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

        public abstract class GetRange
            : DbConnectionExtensionsTests
        {
            public GetRange(DatabaseFixture fixture)
                : base(fixture)
            {
            }

            [Fact]
            public void Filters_result_by_conditions()
            {
                // Arrange
                this.connection.Insert(new User { Name = "Some Name 1", Age = 10 }, dialect: this.dialect);
                this.connection.Insert(new User { Name = "Some Name 2", Age = 10 }, dialect: this.dialect);
                this.connection.Insert(new User { Name = "Some Name 3", Age = 10 }, dialect: this.dialect);
                this.connection.Insert(new User { Name = "Some Name 4", Age = 11 }, dialect: this.dialect);

                // Act
                var users = this.connection.GetRange<User>(
                    "WHERE Name LIKE CONCAT(@Search, '%') and Age = @Age",
                    new { Search = "Some Name", Age = 10 },
                    dialect: this.dialect);

                // Assert
                users.Count().Should().Be(3);

                // Cleanup
                this.connection.DeleteAll<User>(dialect: this.dialect);
            }

            [Fact]
            public void Returns_everything_when_conditions_is_null()
            {
                // Arrange
                this.connection.Insert(new User { Name = "Some Name 1", Age = 10 }, dialect: this.dialect);
                this.connection.Insert(new User { Name = "Some Name 2", Age = 10 }, dialect: this.dialect);
                this.connection.Insert(new User { Name = "Some Name 3", Age = 10 }, dialect: this.dialect);
                this.connection.Insert(new User { Name = "Some Name 4", Age = 11 }, dialect: this.dialect);

                // Act
                var users = this.connection.GetRange<User>(null, dialect: this.dialect);

                // Assert
                users.Count().Should().Be(4);

                // Cleanup
                this.connection.DeleteAll<User>(dialect: this.dialect);
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
            : DbConnectionExtensionsTests
        {
            public GetRangeWhereObject(DatabaseFixture fixture)
                : base(fixture)
            {
            }

            [Fact]
            public void Throws_exception_when_conditions_is_null()
            {
                // Act
                Assert.Throws<ArgumentNullException>(() => this.connection.GetRange<User>((object)null, dialect: this.dialect));
            }

            [Fact]
            public void Returns_all_when_conditions_is_empty()
            {
                // Arrange
                this.connection.Insert(new User { Name = "Some Name 1", Age = 10 }, dialect: this.dialect);
                this.connection.Insert(new User { Name = "Some Name 2", Age = 10 }, dialect: this.dialect);
                this.connection.Insert(new User { Name = "Some Name 3", Age = 10 }, dialect: this.dialect);
                this.connection.Insert(new User { Name = "Some Name 4", Age = 11 }, dialect: this.dialect);

                // Act
                var users = this.connection.GetRange<User>(new { }, dialect: this.dialect);

                // Assert
                users.Count().Should().Be(4);

                // Cleanup
                this.connection.DeleteAll<User>(dialect: this.dialect);
            }

            [Fact]
            public void Filters_result_by_conditions()
            {
                // Arrange
                this.connection.Insert(new User { Name = "Some Name 1", Age = 10 }, dialect: this.dialect);
                this.connection.Insert(new User { Name = "Some Name 2", Age = 10 }, dialect: this.dialect);
                this.connection.Insert(new User { Name = "Some Name 3", Age = 10 }, dialect: this.dialect);
                this.connection.Insert(new User { Name = "Some Name 4", Age = 11 }, dialect: this.dialect);

                // Act
                var users = this.connection.GetRange<User>(new { Age = 10 }, dialect: this.dialect);

                // Assert
                users.Count().Should().Be(3);

                // Cleanup
                this.connection.DeleteAll<User>(dialect: this.dialect);
            }

            [Fact]
            public void When_value_is_not_null_does_not_find_nulls()
            {
                // Arrange
                this.connection.Insert(new PropertyNullable { Name = null }, dialect: this.dialect);
                this.connection.Insert(new PropertyNullable { Name = "Some Name 3" }, dialect: this.dialect);
                this.connection.Insert(new PropertyNullable { Name = null }, dialect: this.dialect);

                // Act
                var users = this.connection.GetRange<PropertyNullable>(new { Name = "Some Name 3" }, dialect: this.dialect);

                // Assert
                users.Count().Should().Be(1);

                // Cleanup
                this.connection.DeleteAll<PropertyNullable>(dialect: this.dialect);
            }

            [Fact]
            public void When_value_is_null_finds_nulls()
            {
                // Arrange
                this.connection.Insert(new PropertyNullable { Name = null }, dialect: this.dialect);
                this.connection.Insert(new PropertyNullable { Name = "Some Name 3" }, dialect: this.dialect);
                this.connection.Insert(new PropertyNullable { Name = null }, dialect: this.dialect);

                // Act
                var users = this.connection.GetRange<PropertyNullable>(new { Name = (string)null }, dialect: this.dialect);

                // Assert
                users.Count().Should().Be(2);

                // Cleanup
                this.connection.DeleteAll<PropertyNullable>(dialect: this.dialect);
            }

            [Fact]
            public void Filters_on_multiple_properties()
            {
                // Arrange
                this.connection.Insert(new User { Name = "Some Name 1", Age = 10 }, dialect: this.dialect);
                this.connection.Insert(new User { Name = "Some Name 2", Age = 10 }, dialect: this.dialect);
                this.connection.Insert(new User { Name = "Some Name 2", Age = 12 }, dialect: this.dialect);

                // Act
                var users = this.connection.GetRange<User>(new { Name = "Some Name 2", Age = 10 }, dialect: this.dialect);

                // Assert
                users.Count().Should().Be(1);

                // Cleanup
                this.connection.DeleteAll<User>(dialect: this.dialect);
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
            : DbConnectionExtensionsTests
        {
            public GetPage(DatabaseFixture fixture)
                : base(fixture)
            {
            }

            [Fact]
            public void Returns_empty_list_when_there_are_no_entities()
            {
                // Act
                var pageBuilder = new PageIndexPageBuilder(1, 10);
                var users = this.connection.GetPage<User>(pageBuilder, null, "Age", (object)null, dialect: this.dialect);

                // Assert
                users.Items.Count().Should().Be(0);
            }

            [Fact]
            public void Filters_result_by_conditions()
            {
                // Arrange
                this.connection.Insert(new User { Name = "Some Name 1", Age = 10 }, dialect: this.dialect);
                this.connection.Insert(new User { Name = "Some Name 2", Age = 10 }, dialect: this.dialect);
                this.connection.Insert(new User { Name = "Some Name 3", Age = 10 }, dialect: this.dialect);
                this.connection.Insert(new User { Name = "Some Name 4", Age = 11 }, dialect: this.dialect);

                // Act
                var users = this.connection.GetPage<User>(
                    new PageIndexPageBuilder(1, 10),
                    "WHERE Name LIKE CONCAT(@Search, '%') and Age = @Age",
                    "Age",
                    new { Search = "Some Name", Age = 10 },
                    dialect: this.dialect);

                // Assert
                users.Items.Count().Should().Be(3);

                // Cleanup
                this.connection.DeleteAll<User>(dialect: this.dialect);
            }

            [Fact]
            public void Gets_first_page()
            {
                // Arrange
                this.connection.Insert(new User { Name = "Some Name 1", Age = 10 }, dialect: this.dialect);
                this.connection.Insert(new User { Name = "Some Name 2", Age = 10 }, dialect: this.dialect);
                this.connection.Insert(new User { Name = "Some Name 3", Age = 10 }, dialect: this.dialect);
                this.connection.Insert(new User { Name = "Some Name 4", Age = 11 }, dialect: this.dialect);

                // Act
                var users = this.connection.GetPage<User>(
                    new PageIndexPageBuilder(1, 2),
                    "WHERE Name LIKE CONCAT(@Search, '%') and Age = @Age",
                    "Age DESC",
                    new { Search = "Some Name", Age = 10 },
                    dialect: this.dialect).Items;

                // Assert
                users.Count().Should().Be(2);
                users[0].Name.Should().Be("Some Name 1");
                users[1].Name.Should().Be("Some Name 2");

                // Cleanup
                this.connection.DeleteAll<User>(dialect: this.dialect);
            }

            [Fact]
            public void Gets_second_page()
            {
                // Arrange
                this.connection.Insert(new User { Name = "Some Name 1", Age = 10 }, dialect: this.dialect);
                this.connection.Insert(new User { Name = "Some Name 2", Age = 10 }, dialect: this.dialect);
                this.connection.Insert(new User { Name = "Some Name 3", Age = 10 }, dialect: this.dialect);
                this.connection.Insert(new User { Name = "Some Name 4", Age = 11 }, dialect: this.dialect);

                // Act
                var users = this.connection.GetPage<User>(
                    new PageIndexPageBuilder(2, 2),
                    "WHERE Name LIKE CONCAT(@Search, '%') and Age = @Age",
                    "Age DESC",
                    new { Search = "Some Name", Age = 10 },
                    dialect: this.dialect).Items;

                // Assert
                users.Count().Should().Be(1);
                users[0].Name.Should().Be("Some Name 3");

                // Cleanup
                this.connection.DeleteAll<User>(dialect: this.dialect);
            }

            [Fact]
            public void Returns_empty_set_past_last_page()
            {
                // Arrange
                this.connection.Insert(new User { Name = "Some Name 1", Age = 10 }, dialect: this.dialect);
                this.connection.Insert(new User { Name = "Some Name 2", Age = 10 }, dialect: this.dialect);
                this.connection.Insert(new User { Name = "Some Name 3", Age = 10 }, dialect: this.dialect);
                this.connection.Insert(new User { Name = "Some Name 4", Age = 11 }, dialect: this.dialect);

                // Act
                var users = this.connection.GetPage<User>(
                    new PageIndexPageBuilder(3, 2),
                    "WHERE Name LIKE CONCAT(@Search, '%') and Age = @Age",
                    "Age DESC",
                    new { Search = "Some Name", Age = 10 },
                    dialect: this.dialect).Items;

                // Assert
                users.Should().BeEmpty();

                // Cleanup
                this.connection.DeleteAll<User>(dialect: this.dialect);
            }

            [Fact]
            public void Returns_page_from_everything_when_conditions_is_null()
            {
                // Arrange
                this.connection.Insert(new User { Name = "Some Name 1", Age = 10 }, dialect: this.dialect);
                this.connection.Insert(new User { Name = "Some Name 2", Age = 10 }, dialect: this.dialect);
                this.connection.Insert(new User { Name = "Some Name 3", Age = 10 }, dialect: this.dialect);
                this.connection.Insert(new User { Name = "Some Name 4", Age = 11 }, dialect: this.dialect);

                // Act
                var users = this.connection.GetPage<User>(new PageIndexPageBuilder(2, 2), null, "Age DESC", (object)null, dialect: this.dialect).Items;

                // Assert
                users.Count().Should().Be(2);

                // Cleanup
                this.connection.DeleteAll<User>(dialect: this.dialect);
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
            : DbConnectionExtensionsTests
        {
            public GetPageWhereObject(DatabaseFixture fixture)
                : base(fixture)
            {
            }

            [Fact]
            public void Returns_empty_list_when_there_are_no_entities()
            {
                // Act
                var pageBuilder = new PageIndexPageBuilder(1, 10);
                var users = this.connection.GetPage<User>(pageBuilder, new { Age = 10 }, "Age", dialect: this.dialect);

                // Assert
                users.Items.Should().BeEmpty();
            }

            [Fact]
            public void Filters_result_by_conditions()
            {
                // Arrange
                this.connection.Insert(new User { Name = "Some Name 1", Age = 10 }, dialect: this.dialect);
                this.connection.Insert(new User { Name = "Some Name 2", Age = 10 }, dialect: this.dialect);
                this.connection.Insert(new User { Name = "Some Name 3", Age = 10 }, dialect: this.dialect);
                this.connection.Insert(new User { Name = "Some Name 4", Age = 11 }, dialect: this.dialect);

                // Act
                var pageBuilder = new PageIndexPageBuilder(1, 10);
                var users = this.connection.GetPage<User>(pageBuilder, new { Age = 10 }, "Age", dialect: this.dialect);

                // Assert
                users.Items.Count().Should().Be(3);

                // Cleanup
                this.connection.DeleteAll<User>(dialect: this.dialect);
            }

            [Fact]
            public void Gets_first_page()
            {
                // Arrange
                this.connection.Insert(new User { Name = "Some Name 1", Age = 10 }, dialect: this.dialect);
                this.connection.Insert(new User { Name = "Some Name 2", Age = 10 }, dialect: this.dialect);
                this.connection.Insert(new User { Name = "Some Name 3", Age = 10 }, dialect: this.dialect);
                this.connection.Insert(new User { Name = "Some Name 4", Age = 11 }, dialect: this.dialect);

                // Act
                var pageBuilder = new PageIndexPageBuilder(1, 2);
                var page = this.connection.GetPage<User>(pageBuilder, new { Age = 10 }, "Age", dialect: this.dialect);
                var users = page.Items;

                // Assert
                users.Count().Should().Be(2);
                users[0].Name.Should().Be("Some Name 1");
                users[1].Name.Should().Be("Some Name 2");

                // Cleanup
                this.connection.DeleteAll<User>(dialect: this.dialect);
            }

            [Fact]
            public void Gets_second_page()
            {
                // Arrange
                this.connection.Insert(new User { Name = "Some Name 1", Age = 10 }, dialect: this.dialect);
                this.connection.Insert(new User { Name = "Some Name 2", Age = 10 }, dialect: this.dialect);
                this.connection.Insert(new User { Name = "Some Name 3", Age = 10 }, dialect: this.dialect);
                this.connection.Insert(new User { Name = "Some Name 4", Age = 11 }, dialect: this.dialect);

                // Act
                var pageBuilder = new PageIndexPageBuilder(2, 2);
                var page = this.connection.GetPage<User>(pageBuilder, new { Age = 10 }, "Age", dialect: this.dialect);
                var users = page.Items;

                // Assert
                users.Count().Should().Be(1);
                users[0].Name.Should().Be("Some Name 3");

                // Cleanup
                this.connection.DeleteAll<User>(dialect: this.dialect);
            }

            [Fact]
            public void Returns_empty_set_past_last_page()
            {
                // Arrange
                this.connection.Insert(new User { Name = "Some Name 1", Age = 10 }, dialect: this.dialect);
                this.connection.Insert(new User { Name = "Some Name 2", Age = 10 }, dialect: this.dialect);
                this.connection.Insert(new User { Name = "Some Name 3", Age = 10 }, dialect: this.dialect);
                this.connection.Insert(new User { Name = "Some Name 4", Age = 11 }, dialect: this.dialect);

                // Act
                var pageBuilder = new PageIndexPageBuilder(3, 2);
                var page = this.connection.GetPage<User>(pageBuilder, new { Age = 10 }, "Age", dialect: this.dialect);
                var users = page.Items;

                // Assert
                users.Should().BeEmpty();

                // Cleanup
                this.connection.DeleteAll<User>(dialect: this.dialect);
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
            : DbConnectionExtensionsTests
        {
            public GetAll(DatabaseFixture fixture)
                : base(fixture)
            {
            }

            [Fact]
            public void Gets_all()
            {
                // Arrange
                this.connection.Insert(new User { Name = "Some Name 1", Age = 10 }, dialect: this.dialect);
                this.connection.Insert(new User { Name = "Some Name 2", Age = 10 }, dialect: this.dialect);
                this.connection.Insert(new User { Name = "Some Name 3", Age = 10 }, dialect: this.dialect);
                this.connection.Insert(new User { Name = "Some Name 4", Age = 11 }, dialect: this.dialect);

                // Act
                var users = this.connection.GetAll<User>(dialect: this.dialect);

                // Assert
                users.Count().Should().Be(4);

                // Cleanup
                this.connection.DeleteAll<User>(dialect: this.dialect);
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
            : DbConnectionExtensionsTests
        {
            public Insert(DatabaseFixture fixture)
                : base(fixture)
            {
            }

            [Fact]
            public void Inserts_entity_with_int32_key()
            {
                // Arrange
                var entity = new KeyInt32 { Name = "Some Name" };

                // Act
                this.connection.Insert(entity, dialect: this.dialect);

                // Assert
                this.connection.Count<KeyInt32>(dialect: this.dialect).Should().Be(1);

                // Cleanup
                this.connection.DeleteAll<KeyInt32>(dialect: this.dialect);
            }

            [Fact]
            public void Inserts_entity_with_int64_key()
            {
                // Arrange
                var entity = new KeyInt64 { Name = "Some Name" };

                // Act
                this.connection.Insert(entity, dialect: this.dialect);

                // Assert
                this.connection.Count<KeyInt64>(dialect: this.dialect).Should().Be(1);

                // Cleanup
                this.connection.DeleteAll<KeyInt64>(dialect: this.dialect);
            }

            [Fact]
            public void Inserts_entities_with_composite_keys()
            {
                // Arrange
                var entity = new CompositeKeys { Key1 = 2, Key2 = 3, Name = "Some Name" };

                // Act
                this.connection.Insert(entity, dialect: this.dialect);

                // Assert
                this.connection.Count<CompositeKeys>(dialect: this.dialect).Should().Be(1);

                // Cleanup
                this.connection.DeleteAll<CompositeKeys>(dialect: this.dialect);
            }

            [Fact]
            public void Does_not_allow_part_of_composite_key_to_be_null()
            {
                // Arrange
                var entity = new CompositeKeys { Key1 = null, Key2 = 5, Name = "Some Name" };

                // Act
                Action act = () => this.connection.Insert(entity, dialect: this.dialect);

                // Assert
                act.ShouldThrow<Exception>();
            }

            [Fact]
            public void Inserts_entities_with_string_key()
            {
                // Arrange
                var entity = new KeyString { Name = "Some Name", Age = 10 };

                // Act
                this.connection.Insert(entity, dialect: this.dialect);

                // Assert
                this.connection.Count<KeyString>(dialect: this.dialect).Should().Be(1);

                // Cleanup
                this.connection.DeleteAll<KeyString>(dialect: this.dialect);
            }

            [Fact]
            public void Does_not_allow_string_key_to_be_null()
            {
                // Arrange
                var entity = new KeyString { Name = null, Age = 10 };

                // Act
                Action act = () => this.connection.Insert(entity, dialect: this.dialect);

                // Assert
                act.ShouldThrow<Exception>();
            }

            [Fact]
            public void Inserts_entities_with_guid_key()
            {
                // Arrange
                var entity = new KeyGuid { Id = Guid.NewGuid(), Name = "Some Name" };

                // Act
                this.connection.Insert(entity, dialect: this.dialect);

                // Assert
                this.connection.Count<KeyGuid>(dialect: this.dialect).Should().Be(1);

                // Cleanup
                this.connection.DeleteAll<KeyGuid>(dialect: this.dialect);
            }

            [Fact]
            public void Uses_key_attribute_to_determine_key()
            {
                // Arrange
                var entity = new KeyAlias { Name = "Some Name" };

                // Act
                this.connection.Insert(entity, dialect: this.dialect);

                // Assert
                this.connection.Count<KeyAlias>(dialect: this.dialect).Should().Be(1);

                // Cleanup
                this.connection.DeleteAll<KeyAlias>(dialect: this.dialect);
            }

            [Fact]
            public void Inserts_into_other_schemas()
            {
                // Arrange
                var entity = new SchemaOther { Name = "Some name" };

                // Act
                this.connection.Insert(entity, dialect: this.dialect);

                // Assert
                this.connection.Count<SchemaOther>(dialect: this.dialect).Should().Be(1);

                // Cleanup
                this.connection.DeleteAll<SchemaOther>(dialect: this.dialect);
            }

            [Fact]
            public void Ignores_columns_which_are_not_mapped()
            {
                // Arrange
                var entity = new PropertyNotMapped { Firstname = "Bobby", LastName = "DropTables", Age = 10 };

                // Act
                this.connection.Insert(entity, dialect: this.dialect);

                // Assert
                this.connection.Count<PropertyNotMapped>(dialect: this.dialect).Should().Be(1);

                // Cleanup
                this.connection.DeleteAll<PropertyNotMapped>(dialect: this.dialect);
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
            : DbConnectionExtensionsTests
        {
            public InsertAndReturnKey(DatabaseFixture fixture)
                : base(fixture)
            {
            }

            [Fact]
            public void Throws_exception_when_entity_has_no_key()
            {
                // Act
                Assert.Throws<InvalidPrimaryKeyException>(
                    () => this.connection.Insert<int>(new NoKey(), dialect: this.dialect));
            }

            [Fact]
            public void Throws_exception_when_entity_has_composite_keys()
            {
                // Act
                Assert.Throws<InvalidPrimaryKeyException>(
                    () => this.connection.Insert<int>(new CompositeKeys(), dialect: this.dialect));
            }

            [Fact]
            public void Throws_exception_for_string_keys()
            {
                // Arrange
                var entity = new KeyString { Name = "Some Name", Age = 10 };

                // Act / Assert
                Assert.Throws<InvalidPrimaryKeyException>(
                    () => this.connection.Insert<string>(entity, dialect: this.dialect));
            }

            [Fact]
            public void Throws_exception_for_guid_keys()
            {
                // Arrange
                var entity = new KeyGuid { Id = Guid.NewGuid(), Name = "Some Name" };

                // Act / Assert
                Assert.Throws<InvalidPrimaryKeyException>(
                    () => this.connection.Insert<Guid>(entity, dialect: this.dialect));
            }

            [Fact]
            public void Inserts_entity_with_int32_primary_key()
            {
                // Act
                var id = this.connection.Insert<int>(new KeyInt32 { Name = "Some Name" }, dialect: this.dialect);

                // Assert
                id.Should().BeGreaterThan(0);

                // Cleanup
                this.connection.Delete<KeyInt32>(id, dialect: this.dialect);
            }

            [Fact]
            public void Inserts_entity_with_int64_primary_key()
            {
                // Act
                var id = this.connection.Insert<int>(new KeyInt64 { Name = "Some Name" }, dialect: this.dialect);

                // Assert
                id.Should().BeGreaterThan(0);

                // Cleanup
                this.connection.Delete<KeyInt64>(id, dialect: this.dialect);
            }

            [Fact]
            public void Uses_key_attribute_to_determine_key()
            {
                // Act
                var id = this.connection.Insert<int>(new KeyAlias { Name = "Some Name" }, dialect: this.dialect);

                // Assert
                id.Should().BeGreaterThan(0);

                // Cleanup
                this.connection.Delete<KeyAlias>(id, dialect: this.dialect);
            }

            [Fact]
            public void Inserts_into_other_schemas()
            {
                // Act
                var id = this.connection.Insert<int>(new SchemaOther { Name = "Some name" }, dialect: this.dialect);

                // Assert
                id.Should().BeGreaterThan(0);

                // Cleanup
                this.connection.Delete<SchemaOther>(id, dialect: this.dialect);
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
            : DbConnectionExtensionsTests
        {
            public InsertRange(DatabaseFixture fixture)
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
                this.connection.InsertRange(entities, dialect: this.dialect);

                // Assert
                this.connection.Count<KeyInt32>(dialect: this.dialect).Should().Be(2);

                // Cleanup
                this.connection.DeleteAll<KeyInt32>(dialect: this.dialect);
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
                this.connection.InsertRange(entities, dialect: this.dialect);

                // Assert
                this.connection.Count<KeyInt64>(dialect: this.dialect).Should().Be(2);

                // Cleanup
                this.connection.DeleteAll<KeyInt64>(dialect: this.dialect);
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
                this.connection.InsertRange(entities, dialect: this.dialect);

                // Assert
                this.connection.Count<CompositeKeys>(dialect: this.dialect).Should().Be(2);

                // Cleanup
                this.connection.DeleteAll<CompositeKeys>(dialect: this.dialect);
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
                this.connection.InsertRange(entities, dialect: this.dialect);

                // Assert
                this.connection.Count<KeyString>(dialect: this.dialect).Should().Be(2);

                // Cleanup
                this.connection.DeleteAll<KeyString>(dialect: this.dialect);
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
                this.connection.InsertRange(entities, dialect: this.dialect);

                // Assert
                this.connection.Count<KeyGuid>(dialect: this.dialect).Should().Be(2);

                // Cleanup
                this.connection.DeleteAll<KeyGuid>(dialect: this.dialect);
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
                this.connection.InsertRange(entities, dialect: this.dialect);

                // Assert
                this.connection.Count<KeyAlias>(dialect: this.dialect).Should().Be(2);

                // Cleanup
                this.connection.DeleteAll<KeyAlias>(dialect: this.dialect);
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
                this.connection.InsertRange(entities, dialect: this.dialect);

                // Assert
                this.connection.Count<SchemaOther>(dialect: this.dialect).Should().Be(2);

                // Cleanup
                this.connection.DeleteAll<SchemaOther>(dialect: this.dialect);
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
            : DbConnectionExtensionsTests
        {
            public InsertRangeAndSetKey(DatabaseFixture fixture)
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
                    () => this.connection.InsertRange<NoKey, int>(entities, (e, k) => { }, dialect: this.dialect));
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
                    () => this.connection.InsertRange<CompositeKeys, int>(entities, (e, k) => { }, dialect: this.dialect));
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
                    () => this.connection.InsertRange<KeyString, string>(entities, (e, k) => { }, dialect: this.dialect));
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
                        () => this.connection.InsertRange<KeyGuid, Guid>(entities, (e, k) => { }, dialect: this.dialect));
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
                this.connection.InsertRange<KeyInt32, int>(entities, (e, k) => { e.Id = k; }, dialect: this.dialect);

                // Assert
                entities[0].Id.Should().BeGreaterThan(0);
                entities[1].Id.Should().BeGreaterThan(entities[0].Id);
                entities[2].Id.Should().BeGreaterThan(entities[1].Id);

                // Cleanup
                this.connection.DeleteAll<KeyInt32>(dialect: this.dialect);
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
                this.connection.InsertRange<KeyInt64, long>(entities, (e, k) => { e.Id = k; }, dialect: this.dialect);

                // Assert
                entities[0].Id.Should().BeGreaterThan(0);
                entities[1].Id.Should().BeGreaterThan(entities[0].Id);
                entities[2].Id.Should().BeGreaterThan(entities[1].Id);

                // Cleanup
                this.connection.DeleteAll<KeyInt64>(dialect: this.dialect);
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
                this.connection.InsertRange<KeyExplicit, int>(entities, (e, k) => { e.Key = k; }, dialect: this.dialect);

                // Assert
                entities[0].Key.Should().BeGreaterThan(0);

                // Cleanup
                this.connection.DeleteAll<KeyAlias>(dialect: this.dialect);
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
                this.connection.InsertRange<SchemaOther, int>(entities, (e, k) => { e.Id = k; }, dialect: this.dialect);

                // Assert
                entities[0].Id.Should().BeGreaterThan(0);
                entities[1].Id.Should().BeGreaterThan(entities[0].Id);
                entities[2].Id.Should().BeGreaterThan(entities[1].Id);

                // Cleanup
                this.connection.DeleteAll<SchemaOther>(dialect: this.dialect);
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
            : DbConnectionExtensionsTests
        {
            public Update(DatabaseFixture fixture)
                : base(fixture)
            {
            }

            [Fact]
            public void Updates_the_entity()
            {
                // Arrange
                var id = this.connection.Insert<int>(new User { Name = "Some name", Age = 10 }, dialect: this.dialect);

                // Act
                var entity = this.connection.Find<User>(id, dialect: this.dialect);
                entity.Name = "Other name";
                this.connection.Update(entity, dialect: this.dialect);

                // Assert
                var updatedEntity = this.connection.Find<User>(id, dialect: this.dialect);
                updatedEntity.Name.Should().Be("Other name");

                // Cleanup
                this.connection.Delete<User>(id, dialect: this.dialect);
            }

            [Fact]
            public void Ignores_columns_which_are_not_mapped()
            {
                // Arrange
                var entity = new PropertyNotMapped { Firstname = "Bobby", LastName = "DropTables", Age = 10 };
                entity.Id = this.connection.Insert<int>(entity, dialect: this.dialect);

                // Act
                entity.LastName = "Other name";
                this.connection.Update(entity, dialect: this.dialect);

                // Assert
                var updatedEntity = this.connection.Find<PropertyNotMapped>(entity.Id, dialect: this.dialect);
                updatedEntity.LastName.Should().Be("Other name");

                // Cleanup
                this.connection.DeleteAll<PropertyNotMapped>(dialect: this.dialect);
            }

            [Fact]
            public void Updates_entities_with_composite_keys()
            {
                // Arrange
                var entity = new CompositeKeys { Key1 = 5, Key2 = 20, Name = "Some name" };
                this.connection.Insert(entity, dialect: this.dialect);

                // Act
                entity.Name = "Other name";
                this.connection.Update(entity, dialect: this.dialect);

                // Assert
                var id = new { Key1 = 5, Key2 = 20 };
                var updatedEntity = this.connection.Find<CompositeKeys>(id, dialect: this.dialect);

                updatedEntity.Name.Should().Be("Other name");

                // Cleanup
                this.connection.DeleteAll<CompositeKeys>(dialect: this.dialect);
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
            : DbConnectionExtensionsTests
        {
            public UpdateRange(DatabaseFixture fixture)
                : base(fixture)
            {
            }

            [Fact]
            public void Updates_the_entity()
            {
                // Arrange
                this.connection.InsertRange(
                    new[]
                        {
                            new User { Name = "Some name1", Age = 10 },
                            new User { Name = "Some name2", Age = 10 },
                            new User { Name = "Some name2", Age = 11 }
                        },
                    dialect: this.dialect);

                // Act
                var entities = this.connection.GetRange<User>("WHERE Age = 10", dialect: this.dialect).ToList();
                foreach (var entity in entities)
                {
                    entity.Name = "Other name";
                }

                var result = this.connection.UpdateRange(entities, dialect: this.dialect);

                // Assert
                result.NumRowsAffected.Should().Be(2);

                var updatedEntities = this.connection.GetRange<User>("WHERE Name = 'Other name'", dialect: this.dialect);
                updatedEntities.Count().Should().Be(2);

                // Cleanup
                this.connection.DeleteAll<User>(dialect: this.dialect);
            }

            [Fact]
            public void Updates_entities_with_composite_keys()
            {
                // Arrange
                this.connection.InsertRange(
                    new[]
                        {
                            new CompositeKeys { Key1 = 5, Key2 = 20, Name = "Some name1" },
                            new CompositeKeys { Key1 = 6, Key2 = 21, Name = "Some name2" },
                            new CompositeKeys { Key1 = 7, Key2 = 22, Name = "Some other name" }
                        },
                    dialect: this.dialect);

                // Act
                var entities = this.connection.GetRange<CompositeKeys>(
                    "WHERE Name Like 'Some name%'",
                    dialect: this.dialect).ToList();

                foreach (var entity in entities)
                {
                    entity.Name = "Other name";
                }

                var result = this.connection.UpdateRange(entities, dialect: this.dialect);

                // Assert
                result.NumRowsAffected.Should().Be(2);

                var updatedEntities = this.connection.GetRange<CompositeKeys>(
                    "WHERE Name = 'Other name'",
                    dialect: this.dialect);
                updatedEntities.Count().Should().Be(2);

                // Cleanup
                this.connection.DeleteAll<CompositeKeys>(dialect: this.dialect);
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
            : DbConnectionExtensionsTests
        {
            public DeleteId(DatabaseFixture fixture)
                : base(fixture)
            {
            }

            [Fact]
            public void Deletes_the_entity_with_the_specified_id()
            {
                // Arrange
                var id = this.connection.Insert<int>(new User { Name = "Some name", Age = 10 }, dialect: this.dialect);

                // Act
                this.connection.Delete<User>(id, dialect: this.dialect);

                // Assert
                this.connection.Find<User>(id, dialect: this.dialect).Should().BeNull();
            }

            [Fact]
            public void Deletes_entity_with_string_key()
            {
                // Arrange
                this.connection.Insert(new KeyString { Name = "Some Name", Age = 10 }, dialect: this.dialect);

                // Act
                this.connection.Delete<KeyString>("Some Name", dialect: this.dialect);
            }

            [Fact]
            public void Deletes_entity_with_guid_key()
            {
                // Arrange
                var id = Guid.NewGuid();
                this.connection.Insert(new KeyGuid { Id = id, Name = "Some Name" }, dialect: this.dialect);

                // Act
                this.connection.Delete<KeyGuid>(id, dialect: this.dialect);
            }

            [Fact]
            public void Deletes_entity_with_composite_keys()
            {
                // Arrange
                var id = new { Key1 = 5, Key2 = 20 };
                var entity = new CompositeKeys { Key1 = 5, Key2 = 20, Name = "Some Name" };
                this.connection.Insert(entity, dialect: this.dialect);

                // Act
                this.connection.Delete<CompositeKeys>(id, dialect: this.dialect);
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
            : DbConnectionExtensionsTests
        {
            public DeleteEntity(DatabaseFixture fixture)
                : base(fixture)
            {
            }

            [Fact]
            public void Deletes_entity_with_matching_key()
            {
                // Arrange
                var id = this.connection.Insert<int>(new User { Name = "Some name", Age = 10 }, dialect: this.dialect);

                // Act
                var entity = this.connection.Find<User>(id, dialect: this.dialect);
                this.connection.Delete(entity, dialect: this.dialect);

                // Assert
                this.connection.Find<User>(id, dialect: this.dialect).Should().BeNull();
            }

            [Fact]
            public void Deletes_entity_with_composite_keys()
            {
                // Arrange
                var id = new { Key1 = 5, Key2 = 20 };
                var entity = new CompositeKeys { Key1 = 5, Key2 = 20, Name = "Some Name" };
                this.connection.Insert(entity, dialect: this.dialect);

                // Act
                this.connection.Delete(entity, dialect: this.dialect);

                // Assert
                this.connection.Find<CompositeKeys>(id, dialect: this.dialect).Should().BeNull();
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
            : DbConnectionExtensionsTests
        {
            public DeleteRange(DatabaseFixture fixture)
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
                    () => this.connection.DeleteRange<User>(conditions, dialect: this.dialect));
            }

            [Theory]
            [InlineData("Where Age = 10")]
            [InlineData("where Age = 10")]
            [InlineData("WHERE Age = 10")]
            public void Allows_any_capitalization_of_where_clause(string conditions)
            {
                // Act
                Action act = () => this.connection.DeleteRange<User>(conditions, dialect: this.dialect);

                // Assert
                act.ShouldNotThrow();
            }

            [Fact]
            public void Deletes_all_matching_entities()
            {
                // Arrange
                this.connection.Insert(new User { Name = "Some Name 1", Age = 10 }, dialect: this.dialect);
                this.connection.Insert(new User { Name = "Some Name 2", Age = 10 }, dialect: this.dialect);
                this.connection.Insert(new User { Name = "Some Name 3", Age = 10 }, dialect: this.dialect);
                this.connection.Insert(new User { Name = "Some Name 4", Age = 11 }, dialect: this.dialect);

                // Act
                var result = this.connection.DeleteRange<User>(
                    "WHERE Age = @Age",
                    new { Age = 10 },
                    dialect: this.dialect);

                // Assert
                result.NumRowsAffected.Should().Be(3);
                this.connection.Count<User>(dialect: this.dialect).Should().Be(1);

                // Cleanup
                this.connection.DeleteAll<User>(dialect: this.dialect);
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
            : DbConnectionExtensionsTests
        {
            public DeleteRangeWhereObject(DatabaseFixture fixture)
                : base(fixture)
            {
            }

            [Fact]
            public void Throws_exception_if_conditions_is_null()
            {
                // Act / Assert
                Assert.Throws<ArgumentNullException>(() => this.connection.DeleteRange<User>((object)null, dialect: this.dialect));
            }

            [Fact]
            public void Throws_exception_if_conditions_is_empty()
            {
                // Act / Assert
                Assert.Throws<ArgumentException>(() => this.connection.DeleteRange<User>(new { }, dialect: this.dialect));
            }

            [Fact]
            public void Deletes_all_matching_entities()
            {
                // Arrange
                this.connection.Insert(new User { Name = "Some Name 1", Age = 10 }, dialect: this.dialect);
                this.connection.Insert(new User { Name = "Some Name 2", Age = 10 }, dialect: this.dialect);
                this.connection.Insert(new User { Name = "Some Name 3", Age = 10 }, dialect: this.dialect);
                this.connection.Insert(new User { Name = "Some Name 4", Age = 11 }, dialect: this.dialect);

                // Act
                var result = this.connection.DeleteRange<User>(new { Age = 10 }, dialect: this.dialect);

                // Assert
                result.NumRowsAffected.Should().Be(3);
                this.connection.Count<User>(dialect: this.dialect).Should().Be(1);

                // Cleanup
                this.connection.DeleteAll<User>(dialect: this.dialect);
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
            : DbConnectionExtensionsTests
        {
            public DeleteAll(DatabaseFixture fixture)
                : base(fixture)
            {
            }

            [Fact]
            public void Deletes_all_entities()
            {
                // Arrange
                this.connection.Insert(new User { Name = "Some Name 1", Age = 10 }, dialect: this.dialect);
                this.connection.Insert(new User { Name = "Some Name 2", Age = 10 }, dialect: this.dialect);
                this.connection.Insert(new User { Name = "Some Name 3", Age = 10 }, dialect: this.dialect);
                this.connection.Insert(new User { Name = "Some Name 4", Age = 11 }, dialect: this.dialect);

                // Act
                var result = this.connection.DeleteAll<User>(dialect: this.dialect);

                // Assert
                result.NumRowsAffected.Should().Be(4);
                this.connection.Count<User>(dialect: this.dialect).Should().Be(0);
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