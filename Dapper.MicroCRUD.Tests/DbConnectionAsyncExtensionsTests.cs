// <copyright file="DbConnectionAsyncExtensionsTests.cs" company="Berkeleybross">
//   Copyright (c) Berkeleybross. All rights reserved.
// </copyright>
namespace Dapper.MicroCRUD.Tests
{
    using System;
    using System.Data;
    using System.Linq;
    using System.Threading.Tasks;
    using Dapper.MicroCRUD.Dialects;
    using Dapper.MicroCRUD.Schema;
    using Dapper.MicroCRUD.Tests.ExampleEntities;
    using Dapper.MicroCRUD.Tests.Utils;
    using NCrunch.Framework;
    using NUnit.Framework;

    [ExclusivelyUses("Database")]
    [Parallelizable(ParallelScope.None)]
    [TestFixtureSource(typeof(BlankDatabaseFactory), nameof(BlankDatabaseFactory.PossibleDialects))]
    public class DbConnectionAsyncExtensionsTests
    {
        private readonly string dialectName;

        private IDbConnection connection;
        private IDialect dialect;
        private BlankDatabase database;

        public DbConnectionAsyncExtensionsTests(string dialectName)
        {
            this.dialectName = dialectName;
        }

        [OneTimeSetUp]
        public void OneTimeSetup()
        {
            this.database = BlankDatabaseFactory.MakeDatabase(this.dialectName);
            this.connection = this.database.Connection;
            this.dialect = this.database.Dialect;
        }

        [OneTimeTearDown]
        public void OneTimeTearDown()
        {
            this.database?.Dispose();
        }

        private class MiscAsync
            : DbConnectionAsyncExtensionsTests
        {
            public MiscAsync(string dialectName)
                : base(dialectName)
            {
            }

            [Test]
            public void Is_in_same_namespace_as_dapper()
            {
                // Assert
                var dapperType = typeof(SqlMapper);
                var sutType = typeof(DbConnectionAsyncExtensions);

                Assert.That(sutType.Namespace, Is.EqualTo(dapperType.Namespace));
            }
        }

        private class CountAsync
            : DbConnectionAsyncExtensionsTests
        {
            public CountAsync(string dialectName)
                : base(dialectName)
            {
            }

            [Test]
            public async Task Counts_entities()
            {
                // Arrange
                this.connection.Insert(new User { Name = "Some Name 1", Age = 10 }, dialect: this.dialect);
                this.connection.Insert(new User { Name = "Some Name 2", Age = 10 }, dialect: this.dialect);
                this.connection.Insert(new User { Name = "Some Name 3", Age = 10 }, dialect: this.dialect);
                this.connection.Insert(new User { Name = "Some Name 4", Age = 11 }, dialect: this.dialect);

                // Act
                var result = await this.connection.CountAsync<User>(dialect: this.dialect);

                // Assert
                Assert.AreEqual(4, result);

                // Cleanup
                this.connection.DeleteAll<User>(dialect: this.dialect);
            }

            [Test]
            public async Task Counts_entities_matching_conditions()
            {
                // Arrange
                this.connection.Insert(new User { Name = "Some Name 1", Age = 10 }, dialect: this.dialect);
                this.connection.Insert(new User { Name = "Some Name 2", Age = 10 }, dialect: this.dialect);
                this.connection.Insert(new User { Name = "Some Name 3", Age = 10 }, dialect: this.dialect);
                this.connection.Insert(new User { Name = "Some Name 4", Age = 11 }, dialect: this.dialect);

                // Act
                var result = await this.connection.CountAsync<User>(
                    "WHERE Age < @Age",
                    new { Age = 11 },
                    dialect: this.dialect);

                // Assert
                Assert.AreEqual(3, result);

                // Cleanup
                this.connection.DeleteAll<User>(dialect: this.dialect);
            }

            [Test]
            public async Task Counts_entities_in_alternate_schema()
            {
                this.connection.Insert<int>(new SchemaOther { Name = "Some Name" }, dialect: this.dialect);
                this.connection.Insert<int>(new SchemaOther { Name = "Some Name" }, dialect: this.dialect);
                this.connection.Insert<int>(new SchemaOther { Name = "Some Name" }, dialect: this.dialect);
                this.connection.Insert<int>(new SchemaOther { Name = "Some Name" }, dialect: this.dialect);

                // Act
                var result = await this.connection.CountAsync<SchemaOther>(dialect: this.dialect);

                // Assert
                Assert.AreEqual(4, result);

                // Cleanup
                this.connection.DeleteAll<SchemaOther>(dialect: this.dialect);
            }
        }

        private class FindAsync
            : DbConnectionAsyncExtensionsTests
        {
            public FindAsync(string dialectName)
                : base(dialectName)
            {
            }

            [Test]
            public void Throws_exception_when_entity_has_no_key()
            {
                // Arrange
                this.connection.Insert(new NoKey { Name = "Some Name", Age = 1 }, dialect: this.dialect);

                // Act
                Assert.ThrowsAsync<InvalidPrimaryKeyException>(
                    async () => await this.connection.FindAsync<NoKey>("Some Name", dialect: this.dialect));
            }

            [Test]
            public async Task Returns_null_when_entity_is_not_found()
            {
                // Act
                var entity = await this.connection.FindAsync<KeyInt32>(12, dialect: this.dialect);

                // Assert
                Assert.IsNull(entity);
            }

            [Test]
            public async Task Finds_entity_by_Int32_primary_key()
            {
                // Arrange
                var id = this.connection.Insert<int>(new KeyInt32 { Name = "Some Name" }, dialect: this.dialect);

                // Act
                var entity = await this.connection.FindAsync<KeyInt32>(id, dialect: this.dialect);

                // Assert
                Assert.That(entity.Name, Is.EqualTo("Some Name"));

                // Cleanup
                this.connection.Delete<KeyInt32>(id, dialect: this.dialect);
            }

            [Test]
            public async Task Finds_entity_by_Int64_primary_key()
            {
                // Arrange
                var id = this.connection.Insert<long>(new KeyInt64 { Name = "Some Name" }, dialect: this.dialect);

                // Act
                var user = await this.connection.FindAsync<KeyInt64>(id, dialect: this.dialect);

                // Assert
                Assert.That(user.Name, Is.EqualTo("Some Name"));

                // Cleanup
                this.connection.Delete<KeyInt64>(id, dialect: this.dialect);
            }

            [Test]
            public async Task Finds_entity_by_string_primary_key()
            {
                // Arrange
                this.connection.Insert(new KeyString { Name = "Some Name", Age = 42 }, dialect: this.dialect);

                // Act
                var entity = await this.connection.FindAsync<KeyString>("Some Name", dialect: this.dialect);

                // Assert
                Assert.That(entity.Age, Is.EqualTo(42));

                // Cleanup
                this.connection.Delete(entity, dialect: this.dialect);
            }

            [Test]
            public async Task Finds_entity_by_guid_primary_key()
            {
                // Arrange
                var id = Guid.NewGuid();
                this.connection.Insert(new KeyGuid { Id = id, Name = "Some Name" }, dialect: this.dialect);

                // Act
                var entity = await this.connection.FindAsync<KeyGuid>(id, dialect: this.dialect);

                // Assert
                Assert.That(entity.Name, Is.EqualTo("Some Name"));

                // Cleanup
                this.connection.Delete(entity, dialect: this.dialect);
            }

            [Test]
            public async Task Finds_entity_by_composite_key()
            {
                // Arrange
                this.connection.Insert(
                    new CompositeKeys { Key1 = 1, Key2 = 1, Name = "Some Name" },
                    dialect: this.dialect);
                var id = new { Key1 = 1, Key2 = 1 };

                // Act
                var entity = await this.connection.FindAsync<CompositeKeys>(id, dialect: this.dialect);

                // Assert
                Assert.That(entity.Name, Is.EqualTo("Some Name"));

                // Cleanup
                this.connection.DeleteAll<CompositeKeys>(dialect: this.dialect);
            }

            [Test]
            public async Task Finds_entities_in_alternate_schema()
            {
                // Arrange
                var id = this.connection.Insert<int>(new SchemaOther { Name = "Some Name" }, dialect: this.dialect);

                // Act
                var entity = await this.connection.FindAsync<SchemaOther>(id, dialect: this.dialect);

                // Assert
                Assert.That(entity.Name, Is.EqualTo("Some Name"));

                // Cleanup
                this.connection.Delete<SchemaOther>(id, dialect: this.dialect);
            }

            [Test]
            public async Task Finds_entities_with_enum_property()
            {
                // Arrange
                var id = this.connection.Insert<int>(
                    new PropertyEnum { FavoriteColor = PropertyEnum.Color.Green },
                    dialect: this.dialect);

                // Act
                var entity = await this.connection.FindAsync<PropertyEnum>(id, dialect: this.dialect);

                // Assert
                Assert.That(entity.FavoriteColor, Is.EqualTo(PropertyEnum.Color.Green));

                // Cleanup
                this.connection.Delete<PropertyEnum>(id, dialect: this.dialect);
            }

            [Test]
            public async Task Finds_entities_with_all_possible_types()
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
                var entity = await this.connection.FindAsync<PropertyAllPossibleTypes>(id, dialect: this.dialect);

                // Assert
                Assert.AreEqual(-16, entity.Int16Property);
                Assert.AreEqual(-16, entity.NullableInt16Property);
                Assert.AreEqual(-32, entity.Int32Property);
                Assert.AreEqual(-32, entity.NullableInt32Property);
                Assert.AreEqual(-64, entity.Int64Property);
                Assert.AreEqual(-64, entity.NullableInt64Property);
                Assert.AreEqual(1, entity.SingleProperty);
                Assert.AreEqual(1, entity.NullableSingleProperty);
                Assert.AreEqual(2, entity.DoubleProperty);
                Assert.AreEqual(2, entity.NullableDoubleProperty);
                Assert.AreEqual(10, entity.DecimalProperty);
                Assert.AreEqual(10, entity.NullableDecimalProperty);
                Assert.AreEqual(true, entity.BoolProperty);
                Assert.AreEqual(true, entity.NullableBoolProperty);
                Assert.AreEqual("Foo", entity.StringProperty);
                Assert.AreEqual('F', entity.CharProperty);
                Assert.AreEqual('N', entity.NullableCharProperty);
                Assert.AreEqual(new Guid("da8326a1-c703-4a79-9fb2-2909b0f40367"), entity.GuidProperty);
                Assert.AreEqual(new Guid("706e6bcf-4a6d-4d19-91e9-935852140c4d"), entity.NullableGuidProperty);
                Assert.AreEqual(new DateTime(2016, 12, 31), entity.DateTimeProperty);
                Assert.AreEqual(new DateTime(2016, 12, 31), entity.NullableDateTimeProperty);
                Assert.AreEqual(
                    new DateTimeOffset(new DateTime(2016, 12, 31), new TimeSpan(0, 1, 0, 0)),
                    entity.DateTimeOffsetProperty);
                Assert.AreEqual(
                    new DateTimeOffset(new DateTime(2016, 12, 31), new TimeSpan(0, 1, 0, 0)),
                    entity.NullableDateTimeOffsetProperty);
                Assert.AreEqual(new byte[] { 1, 2, 3 }, entity.ByteArrayProperty);

                // Cleanup
                this.connection.Delete<PropertyAllPossibleTypes>(id, dialect: this.dialect);
            }

            [Test]
            public async Task Ignores_columns_which_are_not_mapped()
            {
                // Arrange
                var id = this.connection.Insert<int>(
                    new PropertyNotMapped { Firstname = "Bobby", LastName = "DropTables", Age = 10 },
                    dialect: this.dialect);

                // Act
                var entity = await this.connection.FindAsync<PropertyNotMapped>(id, dialect: this.dialect);

                // Assert
                Assert.That(entity.Firstname, Is.EqualTo("Bobby"));
                Assert.That(entity.LastName, Is.EqualTo("DropTables"));
                Assert.That(entity.FullName, Is.EqualTo("Bobby DropTables"));
                Assert.That(entity.Age, Is.EqualTo(0));

                // Cleanup
                this.connection.DeleteAll<PropertyNotMapped>(dialect: this.dialect);
            }
        }

        private class GetAsync
            : DbConnectionAsyncExtensionsTests
        {
            public GetAsync(string dialectName)
                : base(dialectName)
            {
            }

            [Test]
            public void Throws_exception_when_entity_has_no_key()
            {
                // Arrange
                this.connection.Insert(new NoKey { Name = "Some Name", Age = 1 }, dialect: this.dialect);

                // Act
                Assert.ThrowsAsync<InvalidPrimaryKeyException>(
                    async () => await this.connection.GetAsync<NoKey>("Some Name", dialect: this.dialect));
            }

            [Test]
            public void Throws_exception_when_entity_is_not_found()
            {
                // Act
                Assert.ThrowsAsync<InvalidOperationException>(
                    async () => await this.connection.GetAsync<KeyInt32>(5, dialect: this.dialect));
            }

            [Test]
            public async Task Finds_entity_by_Int32_primary_key()
            {
                // Arrange
                var id = this.connection.Insert<int>(new KeyInt32 { Name = "Some Name" }, dialect: this.dialect);

                // Act
                var entity = await this.connection.GetAsync<KeyInt32>(id, dialect: this.dialect);

                // Assert
                Assert.That(entity.Name, Is.EqualTo("Some Name"));

                // Cleanup
                this.connection.Delete<KeyInt32>(id, dialect: this.dialect);
            }

            [Test]
            public async Task Finds_entity_by_Int64_primary_key()
            {
                // Arrange
                var id = this.connection.Insert<long>(new KeyInt64 { Name = "Some Name" }, dialect: this.dialect);

                // Act
                var user = await this.connection.GetAsync<KeyInt64>(id, dialect: this.dialect);

                // Assert
                Assert.That(user.Name, Is.EqualTo("Some Name"));

                // Cleanup
                this.connection.Delete<KeyInt64>(id, dialect: this.dialect);
            }

            [Test]
            public async Task Finds_entity_by_string_primary_key()
            {
                // Arrange
                this.connection.Insert(new KeyString { Name = "Some Name", Age = 42 }, dialect: this.dialect);

                // Act
                var entity = await this.connection.GetAsync<KeyString>("Some Name", dialect: this.dialect);

                // Assert
                Assert.That(entity.Age, Is.EqualTo(42));

                // Cleanup
                this.connection.Delete(entity, dialect: this.dialect);
            }

            [Test]
            public async Task Finds_entity_by_guid_primary_key()
            {
                // Arrange
                var id = Guid.NewGuid();
                this.connection.Insert(new KeyGuid { Id = id, Name = "Some Name" }, dialect: this.dialect);

                // Act
                var entity = await this.connection.GetAsync<KeyGuid>(id, dialect: this.dialect);

                // Assert
                Assert.That(entity.Name, Is.EqualTo("Some Name"));

                // Cleanup
                this.connection.Delete(entity, dialect: this.dialect);
            }

            [Test]
            public async Task Finds_entity_by_composite_key()
            {
                // Arrange
                this.connection.Insert(
                    new CompositeKeys { Key1 = 1, Key2 = 1, Name = "Some Name" },
                    dialect: this.dialect);
                var id = new { Key1 = 1, Key2 = 1 };

                // Act
                var entity = await this.connection.GetAsync<CompositeKeys>(id, dialect: this.dialect);

                // Assert
                Assert.That(entity.Name, Is.EqualTo("Some Name"));

                // Cleanup
                this.connection.DeleteAll<CompositeKeys>(dialect: this.dialect);
            }

            [Test]
            public async Task Finds_entities_in_alternate_schema()
            {
                // Arrange
                var id = this.connection.Insert<int>(new SchemaOther { Name = "Some Name" }, dialect: this.dialect);

                // Act
                var entity = await this.connection.GetAsync<SchemaOther>(id, dialect: this.dialect);

                // Assert
                Assert.That(entity.Name, Is.EqualTo("Some Name"));

                // Cleanup
                this.connection.Delete<SchemaOther>(id, dialect: this.dialect);
            }

            [Test]
            public async Task Ignores_columns_which_are_not_mapped()
            {
                // Arrange
                var id = this.connection.Insert<int>(
                    new PropertyNotMapped { Firstname = "Bobby", LastName = "DropTables", Age = 10 },
                    dialect: this.dialect);

                // Act
                var entity = await this.connection.GetAsync<PropertyNotMapped>(id, dialect: this.dialect);

                // Assert
                Assert.That(entity.Firstname, Is.EqualTo("Bobby"));
                Assert.That(entity.LastName, Is.EqualTo("DropTables"));
                Assert.That(entity.FullName, Is.EqualTo("Bobby DropTables"));
                Assert.That(entity.Age, Is.EqualTo(0));

                // Cleanup
                this.connection.DeleteAll<PropertyNotMapped>(dialect: this.dialect);
            }
        }

        private class GetRangeAsync
            : DbConnectionAsyncExtensionsTests
        {
            public GetRangeAsync(string dialectName)
                : base(dialectName)
            {
            }

            [Test]
            public async Task Filters_result_by_conditions()
            {
                // Arrange
                this.connection.Insert(new User { Name = "Some Name 1", Age = 10 }, dialect: this.dialect);
                this.connection.Insert(new User { Name = "Some Name 2", Age = 10 }, dialect: this.dialect);
                this.connection.Insert(new User { Name = "Some Name 3", Age = 10 }, dialect: this.dialect);
                this.connection.Insert(new User { Name = "Some Name 4", Age = 11 }, dialect: this.dialect);

                // Act
                var users = await this.connection.GetRangeAsync<User>(
                    "WHERE Name LIKE CONCAT(@Search, '%') and Age = @Age",
                    new { Search = "Some Name", Age = 10 },
                    dialect: this.dialect);

                // Assert
                Assert.That(users.Count(), Is.EqualTo(3));

                // Cleanup
                this.connection.DeleteAll<User>(dialect: this.dialect);
            }

            [Test]
            public async Task Returns_everything_when_conditions_is_null()
            {
                // Arrange
                this.connection.Insert(new User { Name = "Some Name 1", Age = 10 }, dialect: this.dialect);
                this.connection.Insert(new User { Name = "Some Name 2", Age = 10 }, dialect: this.dialect);
                this.connection.Insert(new User { Name = "Some Name 3", Age = 10 }, dialect: this.dialect);
                this.connection.Insert(new User { Name = "Some Name 4", Age = 11 }, dialect: this.dialect);

                // Act
                var users = await this.connection.GetRangeAsync<User>(null, dialect: this.dialect);

                // Assert
                Assert.That(users.Count(), Is.EqualTo(4));

                // Cleanup
                this.connection.DeleteAll<User>(dialect: this.dialect);
            }
        }

        private class GetPageAsync
            : DbConnectionAsyncExtensionsTests
        {
            public GetPageAsync(string dialectName)
                : base(dialectName)
            {
            }

            [Test]
            public async Task Filters_result_by_conditions()
            {
                // Arrange
                this.connection.Insert(new User { Name = "Some Name 1", Age = 10 }, dialect: this.dialect);
                this.connection.Insert(new User { Name = "Some Name 2", Age = 10 }, dialect: this.dialect);
                this.connection.Insert(new User { Name = "Some Name 3", Age = 10 }, dialect: this.dialect);
                this.connection.Insert(new User { Name = "Some Name 4", Age = 11 }, dialect: this.dialect);

                // Act
                var users = await this.connection.GetPageAsync<User>(
                    1,
                    10,
                    "WHERE Name LIKE CONCAT(@Search, '%') and Age = @Age",
                    "Age",
                    new { Search = "Some Name", Age = 10 },
                    dialect: this.dialect);

                // Assert
                Assert.That(users.Count(), Is.EqualTo(3));

                // Cleanup
                this.connection.DeleteAll<User>(dialect: this.dialect);
            }

            [Test]
            public async Task Gets_first_page()
            {
                // Arrange
                this.connection.Insert(new User { Name = "Some Name 1", Age = 10 }, dialect: this.dialect);
                this.connection.Insert(new User { Name = "Some Name 2", Age = 10 }, dialect: this.dialect);
                this.connection.Insert(new User { Name = "Some Name 3", Age = 10 }, dialect: this.dialect);
                this.connection.Insert(new User { Name = "Some Name 4", Age = 11 }, dialect: this.dialect);

                // Act
                var users = (await this.connection.GetPageAsync<User>(
                    1,
                    2,
                    "WHERE Name LIKE CONCAT(@Search, '%') and Age = @Age",
                    "Age DESC",
                    new { Search = "Some Name", Age = 10 },
                    dialect: this.dialect)).ToList();

                // Assert
                Assert.That(users.Count, Is.EqualTo(2));
                Assert.That(users[0].Name, Is.EqualTo("Some Name 1"));
                Assert.That(users[1].Name, Is.EqualTo("Some Name 2"));

                // Cleanup
                this.connection.DeleteAll<User>(dialect: this.dialect);
            }

            [Test]
            public async Task Gets_second_page()
            {
                // Arrange
                this.connection.Insert(new User { Name = "Some Name 1", Age = 10 }, dialect: this.dialect);
                this.connection.Insert(new User { Name = "Some Name 2", Age = 10 }, dialect: this.dialect);
                this.connection.Insert(new User { Name = "Some Name 3", Age = 10 }, dialect: this.dialect);
                this.connection.Insert(new User { Name = "Some Name 4", Age = 11 }, dialect: this.dialect);

                // Act
                var users = (await this.connection.GetPageAsync<User>(
                    2,
                    2,
                    "WHERE Name LIKE CONCAT(@Search, '%') and Age = @Age",
                    "Age DESC",
                    new { Search = "Some Name", Age = 10 },
                    dialect: this.dialect)).ToList();

                // Assert
                Assert.That(users.Count, Is.EqualTo(1));
                Assert.That(users[0].Name, Is.EqualTo("Some Name 3"));

                // Cleanup
                this.connection.DeleteAll<User>(dialect: this.dialect);
            }

            [Test]
            public async Task Returns_empty_set_past_last_page()
            {
                // Arrange
                this.connection.Insert(new User { Name = "Some Name 1", Age = 10 }, dialect: this.dialect);
                this.connection.Insert(new User { Name = "Some Name 2", Age = 10 }, dialect: this.dialect);
                this.connection.Insert(new User { Name = "Some Name 3", Age = 10 }, dialect: this.dialect);
                this.connection.Insert(new User { Name = "Some Name 4", Age = 11 }, dialect: this.dialect);

                // Act
                var users = (await this.connection.GetPageAsync<User>(
                    3,
                    2,
                    "WHERE Name LIKE CONCAT(@Search, '%') and Age = @Age",
                    "Age DESC",
                    new { Search = "Some Name", Age = 10 },
                    dialect: this.dialect)).ToList();

                // Assert
                Assert.That(users, Is.Empty);

                // Cleanup
                this.connection.DeleteAll<User>(dialect: this.dialect);
            }

            [Test]
            public async Task Returns_everything_when_conditions_is_null()
            {
                // Arrange
                this.connection.Insert(new User { Name = "Some Name 1", Age = 10 }, dialect: this.dialect);
                this.connection.Insert(new User { Name = "Some Name 2", Age = 10 }, dialect: this.dialect);
                this.connection.Insert(new User { Name = "Some Name 3", Age = 10 }, dialect: this.dialect);
                this.connection.Insert(new User { Name = "Some Name 4", Age = 11 }, dialect: this.dialect);

                // Act
                var users = await this.connection.GetRangeAsync<User>(null, dialect: this.dialect);

                // Assert
                Assert.That(users.Count(), Is.EqualTo(4));

                // Cleanup
                this.connection.DeleteAll<User>(dialect: this.dialect);
            }
        }

        private class GetAllAsync
            : DbConnectionAsyncExtensionsTests
        {
            public GetAllAsync(string dialectName)
                : base(dialectName)
            {
            }

            [Test]
            public async Task Gets_all()
            {
                // Arrange
                this.connection.Insert(new User { Name = "Some Name 1", Age = 10 }, dialect: this.dialect);
                this.connection.Insert(new User { Name = "Some Name 2", Age = 10 }, dialect: this.dialect);
                this.connection.Insert(new User { Name = "Some Name 3", Age = 10 }, dialect: this.dialect);
                this.connection.Insert(new User { Name = "Some Name 4", Age = 11 }, dialect: this.dialect);

                // Act
                var users = await this.connection.GetAllAsync<User>(dialect: this.dialect);

                // Assert
                Assert.That(users.Count(), Is.EqualTo(4));

                // Cleanup
                this.connection.DeleteAll<User>(dialect: this.dialect);
            }
        }

        private class InsertAsync
            : DbConnectionAsyncExtensionsTests
        {
            public InsertAsync(string dialectName)
                : base(dialectName)
            {
            }

            [Test]
            public async Task Inserts_entity_with_int32_key()
            {
                // Arrange
                var entity = new KeyInt32 { Name = "Some Name" };

                // Act
                await this.connection.InsertAsync(entity, dialect: this.dialect);

                // Assert
                Assert.AreEqual(1, this.connection.Count<KeyInt32>(dialect: this.dialect));

                // Cleanup
                this.connection.DeleteAll<KeyInt32>(dialect: this.dialect);
            }

            [Test]
            public async Task Inserts_entity_with_int64_key()
            {
                // Arrange
                var entity = new KeyInt64 { Name = "Some Name" };

                // Act
                await this.connection.InsertAsync(entity, dialect: this.dialect);

                // Assert
                Assert.AreEqual(1, this.connection.Count<KeyInt64>(dialect: this.dialect));

                // Cleanup
                this.connection.DeleteAll<KeyInt64>(dialect: this.dialect);
            }

            [Test]
            public async Task Inserts_entities_with_composite_keys()
            {
                // Arrange
                var entity = new CompositeKeys { Key1 = 2, Key2 = 3, Name = "Some Name" };

                // Act
                await this.connection.InsertAsync(entity, dialect: this.dialect);

                // Assert
                Assert.AreEqual(1, this.connection.Count<CompositeKeys>(dialect: this.dialect));

                // Cleanup
                this.connection.DeleteAll<CompositeKeys>(dialect: this.dialect);
            }

            [Test]
            public void Does_not_allow_part_of_composite_key_to_be_null()
            {
                // Arrange
                var entity = new CompositeKeys { Key1 = null, Key2 = 5, Name = "Some Name" };

                // Act
                var ex = Assert.CatchAsync(async () => await this.connection.InsertAsync(entity, dialect: this.dialect));

                // Assert
                Assert.That(ex, Is.Not.Null);
            }

            [Test]
            public async Task Inserts_entities_with_string_key()
            {
                // Arrange
                var entity = new KeyString { Name = "Some Name", Age = 10 };

                // Act
                await this.connection.InsertAsync(entity, dialect: this.dialect);

                // Assert
                Assert.AreEqual(1, this.connection.Count<KeyString>(dialect: this.dialect));

                // Cleanup
                this.connection.DeleteAll<KeyString>(dialect: this.dialect);
            }

            [Test]
            public void Does_not_allow_string_key_to_be_null()
            {
                // Arrange
                var entity = new KeyString { Name = null, Age = 10 };

                // Act
                Assert.CatchAsync(async () => await this.connection.InsertAsync(entity, dialect: this.dialect));
            }

            [Test]
            public async Task Inserts_entities_with_guid_key()
            {
                // Arrange
                var entity = new KeyGuid { Id = Guid.NewGuid(), Name = "Some Name" };

                // Act
                await this.connection.InsertAsync(entity, dialect: this.dialect);

                // Assert
                Assert.AreEqual(1, this.connection.Count<KeyGuid>(dialect: this.dialect));

                // Cleanup
                this.connection.DeleteAll<KeyGuid>(dialect: this.dialect);
            }

            [Test]
            public async Task Uses_key_attribute_to_determine_key()
            {
                // Arrange
                var entity = new KeyAlias { Name = "Some Name" };

                // Act
                await this.connection.InsertAsync(entity, dialect: this.dialect);

                // Assert
                Assert.AreEqual(1, this.connection.Count<KeyAlias>(dialect: this.dialect));

                // Cleanup
                this.connection.DeleteAll<KeyAlias>(dialect: this.dialect);
            }

            [Test]
            public async Task Inserts_into_other_schemas()
            {
                // Arrange
                var entity = new SchemaOther { Name = "Some name" };

                // Act
                await this.connection.InsertAsync(entity, dialect: this.dialect);

                // Assert
                Assert.AreEqual(1, this.connection.Count<SchemaOther>(dialect: this.dialect));

                // Cleanup
                this.connection.DeleteAll<SchemaOther>(dialect: this.dialect);
            }

            [Test]
            public async Task Ignores_columns_which_are_not_mapped()
            {
                // Arrange
                var entity = new PropertyNotMapped { Firstname = "Bobby", LastName = "DropTables", Age = 10 };

                // Act
                await this.connection.InsertAsync(entity, dialect: this.dialect);

                // Assert
                Assert.AreEqual(1, this.connection.Count<PropertyNotMapped>(dialect: this.dialect));

                // Cleanup
                this.connection.DeleteAll<PropertyNotMapped>(dialect: this.dialect);
            }
        }

        private class InsertAndReturnKeyAsync
            : DbConnectionAsyncExtensionsTests
        {
            public InsertAndReturnKeyAsync(string dialectName)
                : base(dialectName)
            {
            }

            [Test]
            public void Throws_exception_when_entity_has_no_key()
            {
                // Act
                Assert.ThrowsAsync<InvalidPrimaryKeyException>(
                    async () => await this.connection.InsertAsync<int>(new NoKey(), dialect: this.dialect));
            }

            [Test]
            public void Throws_exception_when_entity_has_composite_keys()
            {
                // Act
                Assert.ThrowsAsync<InvalidPrimaryKeyException>(
                    async () => await this.connection.InsertAsync<int>(new CompositeKeys(), dialect: this.dialect));
            }

            [Test]
            public void Throws_exception_for_string_keys()
            {
                // Arrange
                var entity = new KeyString { Name = "Some Name", Age = 10 };

                // Act / Assert
                Assert.ThrowsAsync<InvalidPrimaryKeyException>(
                    async () => await this.connection.InsertAsync<string>(entity, dialect: this.dialect));
            }

            [Test]
            public void Throws_exception_for_guid_keys()
            {
                // Arrange
                var entity = new KeyGuid { Id = Guid.NewGuid(), Name = "Some Name" };

                // Act / Assert
                Assert.ThrowsAsync<InvalidPrimaryKeyException>(
                    async () => await this.connection.InsertAsync<Guid>(entity, dialect: this.dialect));
            }

            [Test]
            public async Task Inserts_entity_with_int32_primary_key()
            {
                // Act
                var id = await this.connection.InsertAsync<int>(
                    new KeyInt32 { Name = "Some Name" },
                    dialect: this.dialect);

                // Assert
                Assert.That(id, Is.GreaterThan(0));

                // Cleanup
                this.connection.Delete<KeyInt32>(id, dialect: this.dialect);
            }

            [Test]
            public async Task Inserts_entity_with_int64_primary_key()
            {
                // Act
                var id = await this.connection.InsertAsync<int>(
                    new KeyInt64 { Name = "Some Name" },
                    dialect: this.dialect);

                // Assert
                Assert.That(id, Is.GreaterThan(0));

                // Cleanup
                this.connection.Delete<KeyInt64>(id, dialect: this.dialect);
            }

            [Test]
            public async Task Uses_key_attribute_to_determine_key()
            {
                // Act
                var id = await this.connection.InsertAsync<int>(
                    new KeyAlias { Name = "Some Name" },
                    dialect: this.dialect);

                // Assert
                Assert.That(id, Is.GreaterThan(0));

                // Cleanup
                this.connection.Delete<KeyAlias>(id, dialect: this.dialect);
            }

            [Test]
            public async Task Inserts_into_other_schemas()
            {
                // Act
                var id = await this.connection.InsertAsync<int>(
                    new SchemaOther { Name = "Some name" },
                    dialect: this.dialect);

                // Assert
                Assert.That(id, Is.GreaterThan(0));

                // Cleanup
                this.connection.Delete<SchemaOther>(id, dialect: this.dialect);
            }
        }

        private class InsertRangeAsync
            : DbConnectionAsyncExtensionsTests
        {
            public InsertRangeAsync(string dialectName)
                : base(dialectName)
            {
            }

            [Test]
            public async Task Inserts_entity_with_int32_key()
            {
                // Arrange
                var entities = new[]
                    {
                        new KeyInt32 { Name = "Some Name" },
                        new KeyInt32 { Name = "Some Name2" }
                    };

                // Act
                await this.connection.InsertRangeAsync(entities, dialect: this.dialect);

                // Assert
                Assert.AreEqual(2, this.connection.Count<KeyInt32>(dialect: this.dialect));

                // Cleanup
                this.connection.DeleteAll<KeyInt32>(dialect: this.dialect);
            }

            [Test]
            public async Task Inserts_entity_with_int64_key()
            {
                // Arrange
                var entities = new[]
                    {
                        new KeyInt64 { Name = "Some Name" },
                        new KeyInt64 { Name = "Some Name2" }
                    };

                // Act
                await this.connection.InsertRangeAsync(entities, dialect: this.dialect);

                // Assert
                Assert.AreEqual(2, this.connection.Count<KeyInt64>(dialect: this.dialect));

                // Cleanup
                this.connection.DeleteAll<KeyInt64>(dialect: this.dialect);
            }

            [Test]
            public async Task Inserts_entities_with_composite_keys()
            {
                // Arrange
                var entities = new[]
                    {
                        new CompositeKeys { Key1 = 2, Key2 = 3, Name = "Some Name1" },
                        new CompositeKeys { Key1 = 3, Key2 = 3, Name = "Some Name2" }
                    };

                // Act
                await this.connection.InsertRangeAsync(entities, dialect: this.dialect);

                // Assert
                Assert.AreEqual(2, this.connection.Count<CompositeKeys>(dialect: this.dialect));

                // Cleanup
                this.connection.DeleteAll<CompositeKeys>(dialect: this.dialect);
            }

            [Test]
            public async Task Inserts_entities_with_string_key()
            {
                // Arrange
                var entities = new[]
                    {
                        new KeyString { Name = "Some Name", Age = 10 },
                        new KeyString { Name = "Some Name2", Age = 11 }
                    };

                // Act
                await this.connection.InsertRangeAsync(entities, dialect: this.dialect);

                // Assert
                Assert.AreEqual(2, this.connection.Count<KeyString>(dialect: this.dialect));

                // Cleanup
                this.connection.DeleteAll<KeyString>(dialect: this.dialect);
            }

            [Test]
            public async Task Inserts_entities_with_guid_key()
            {
                // Arrange
                var entities = new[]
                    {
                        new KeyGuid { Id = Guid.NewGuid(), Name = "Some Name" },
                        new KeyGuid { Id = Guid.NewGuid(), Name = "Some Name2" }
                    };

                // Act
                await this.connection.InsertRangeAsync(entities, dialect: this.dialect);

                // Assert
                Assert.AreEqual(2, this.connection.Count<KeyGuid>(dialect: this.dialect));

                // Cleanup
                this.connection.DeleteAll<KeyGuid>(dialect: this.dialect);
            }

            [Test]
            public async Task Uses_key_attribute_to_determine_key()
            {
                // Arrange
                var entities = new[]
                    {
                        new KeyAlias { Name = "Some Name" },
                        new KeyAlias { Name = "Some Name2" }
                    };

                // Act
                await this.connection.InsertRangeAsync(entities, dialect: this.dialect);

                // Assert
                Assert.AreEqual(2, this.connection.Count<KeyAlias>(dialect: this.dialect));

                // Cleanup
                this.connection.DeleteAll<KeyAlias>(dialect: this.dialect);
            }

            [Test]
            public async Task Inserts_into_other_schemas()
            {
                // Arrange
                var entities = new[]
                    {
                        new SchemaOther { Name = "Some Name" },
                        new SchemaOther { Name = "Some Name2" }
                    };

                // Act
                await this.connection.InsertRangeAsync(entities, dialect: this.dialect);

                // Assert
                Assert.AreEqual(2, this.connection.Count<SchemaOther>(dialect: this.dialect));

                // Cleanup
                this.connection.DeleteAll<SchemaOther>(dialect: this.dialect);
            }
        }

        private class InsertRangeAndSetKeyAsync
            : DbConnectionAsyncExtensionsTests
        {
            public InsertRangeAndSetKeyAsync(string dialectName)
                : base(dialectName)
            {
            }

            [Test]
            public void Throws_exception_when_entity_has_no_key()
            {
                // Arrange
                var entities = new[]
                    {
                        new NoKey()
                    };

                // Act / Assert
                Action<NoKey, int> setKey = (e, k) => { };
                Assert.ThrowsAsync<InvalidPrimaryKeyException>(
                    async () => await this.connection.InsertRangeAsync(entities, setKey, dialect: this.dialect));
            }

            [Test]
            public void Throws_exception_when_entity_has_composite_keys()
            {
                // Arrange
                var entities = new[]
                    {
                        new CompositeKeys()
                    };

                // Act / Assert
                Action<CompositeKeys, int> setKey = (e, k) => { };
                Assert.ThrowsAsync<InvalidPrimaryKeyException>(
                    async () => await this.connection.InsertRangeAsync(entities, setKey, dialect: this.dialect));
            }

            [Test]
            public void Throws_exception_for_string_keys()
            {
                // Arrange
                var entities = new[]
                    {
                        new KeyString { Name = "Some Name", Age = 10 }
                    };

                // Act / Assert
                Action<KeyString, string> setKey = (e, k) => { };
                Assert.ThrowsAsync<InvalidPrimaryKeyException>(
                    async () => await this.connection.InsertRangeAsync(entities, setKey, dialect: this.dialect));
            }

            [Test]
            public void Throws_exception_for_guid_keys()
            {
                // Arrange
                var entities = new[]
                    {
                        new KeyGuid { Id = Guid.NewGuid(), Name = "Some Name" }
                    };

                // Act / Assert
                Action<KeyGuid, Guid> setKey = (e, k) => { };
                Assert.ThrowsAsync<InvalidPrimaryKeyException>(
                    async () => await this.connection.InsertRangeAsync(entities, setKey, dialect: this.dialect));
            }

            [Test]
            public async Task Inserts_entity_with_int32_primary_key()
            {
                // Arrange
                var entities = new[]
                    {
                        new KeyInt32 { Name = "Some Name" },
                        new KeyInt32 { Name = "Some Name2" },
                        new KeyInt32 { Name = "Some Name3" }
                    };

                // Act
                Action<KeyInt32, int> setKey = (e, k) => { e.Id = k; };
                await this.connection.InsertRangeAsync(entities, setKey, dialect: this.dialect);

                // Assert
                Assert.That(entities[0].Id, Is.GreaterThan(0));
                Assert.That(entities[1].Id, Is.GreaterThan(entities[0].Id));
                Assert.That(entities[2].Id, Is.GreaterThan(entities[1].Id));

                // Cleanup
                this.connection.DeleteAll<KeyInt32>(dialect: this.dialect);
            }

            [Test]
            public async Task Inserts_entity_with_int64_primary_key()
            {
                // Arrange
                var entities = new[]
                    {
                        new KeyInt64 { Name = "Some Name" },
                        new KeyInt64 { Name = "Some Name2" },
                        new KeyInt64 { Name = "Some Name3" }
                    };

                // Act
                Action<KeyInt64, long> setKey = (e, k) => { e.Id = k; };
                await this.connection.InsertRangeAsync(entities, setKey, dialect: this.dialect);

                // Assert
                Assert.That(entities[0].Id, Is.GreaterThan(0));
                Assert.That(entities[1].Id, Is.GreaterThan(entities[0].Id));
                Assert.That(entities[2].Id, Is.GreaterThan(entities[1].Id));

                // Cleanup
                this.connection.DeleteAll<KeyInt64>(dialect: this.dialect);
            }

            [Test]
            public async Task Uses_key_attribute_to_determine_key()
            {
                // Arrange
                var entities = new[]
                    {
                        new KeyAlias { Name = "Some Name" }
                    };

                // Act
                Action<KeyAlias, int> setKey = (e, k) => { e.Key = k; };
                await this.connection.InsertRangeAsync(entities, setKey, dialect: this.dialect);

                // Assert
                Assert.That(entities[0].Key, Is.GreaterThan(0));

                // Cleanup
                this.connection.DeleteAll<KeyAlias>(dialect: this.dialect);
            }

            [Test]
            public async Task Inserts_into_other_schemas()
            {
                // Arrange
                var entities = new[]
                    {
                        new SchemaOther { Name = "Some Name" },
                        new SchemaOther { Name = "Some Name2" },
                        new SchemaOther { Name = "Some Name3" }
                    };

                // Act
                Action<SchemaOther, int> setKey = (e, k) => { e.Id = k; };
                await this.connection.InsertRangeAsync(entities, setKey, dialect: this.dialect);

                // Assert
                Assert.That(entities[0].Id, Is.GreaterThan(0));
                Assert.That(entities[1].Id, Is.GreaterThan(entities[0].Id));
                Assert.That(entities[2].Id, Is.GreaterThan(entities[1].Id));

                // Cleanup
                this.connection.DeleteAll<SchemaOther>(dialect: this.dialect);
            }
        }

        private class UpdateAsync
            : DbConnectionAsyncExtensionsTests
        {
            public UpdateAsync(string dialectName)
                : base(dialectName)
            {
            }

            [Test]
            public async Task Updates_the_entity()
            {
                // Arrange
                var id = this.connection.Insert<int>(new User { Name = "Some name", Age = 10 }, dialect: this.dialect);

                // Act
                var entity = this.connection.Find<User>(id, dialect: this.dialect);
                entity.Name = "Other name";
                await this.connection.UpdateAsync(entity, dialect: this.dialect);

                // Assert
                var updatedEntity = this.connection.Find<User>(id, dialect: this.dialect);
                Assert.That(updatedEntity.Name, Is.EqualTo("Other name"));

                // Cleanup
                this.connection.Delete<User>(id, dialect: this.dialect);
            }

            [Test]
            public async Task Ignores_columns_which_are_not_mapped()
            {
                // Arrange
                var entity = new PropertyNotMapped { Firstname = "Bobby", LastName = "DropTables", Age = 10 };
                entity.Id = this.connection.Insert<int>(entity, dialect: this.dialect);

                // Act
                entity.LastName = "Other name";
                await this.connection.UpdateAsync(entity, dialect: this.dialect);

                // Assert
                var updatedEntity = this.connection.Find<PropertyNotMapped>(entity.Id, dialect: this.dialect);
                Assert.That(updatedEntity.LastName, Is.EqualTo("Other name"));

                // Cleanup
                this.connection.DeleteAll<PropertyNotMapped>(dialect: this.dialect);
            }

            [Test]
            public async Task Updates_entities_with_composite_keys()
            {
                // Arrange
                var entity = new CompositeKeys { Key1 = 5, Key2 = 20, Name = "Some name" };
                this.connection.Insert(entity, dialect: this.dialect);

                // Act
                entity.Name = "Other name";
                await this.connection.UpdateAsync(entity, dialect: this.dialect);

                // Assert
                var id = new { Key1 = 5, Key2 = 20 };
                var updatedEntity = this.connection.Find<CompositeKeys>(id, dialect: this.dialect);

                Assert.That(updatedEntity.Name, Is.EqualTo("Other name"));

                // Cleanup
                this.connection.DeleteAll<CompositeKeys>(dialect: this.dialect);
            }
        }

        private class UpdateRangeAsync
            : DbConnectionAsyncExtensionsTests
        {
            public UpdateRangeAsync(string dialectName)
                : base(dialectName)
            {
            }

            [Test]
            public async Task Updates_the_entity()
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

                var result = await this.connection.UpdateRangeAsync(entities, dialect: this.dialect);

                // Assert
                Assert.That(result.NumRowsAffected, Is.EqualTo(2));

                var updatedEntities = this.connection.GetRange<User>("WHERE Name = 'Other name'", dialect: this.dialect);
                Assert.That(updatedEntities.Count(), Is.EqualTo(2));

                // Cleanup
                this.connection.DeleteAll<User>(dialect: this.dialect);
            }

            [Test]
            public async Task Updates_entities_with_composite_keys()
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

                var result = await this.connection.UpdateRangeAsync(entities, dialect: this.dialect);

                // Assert
                Assert.That(result.NumRowsAffected, Is.EqualTo(2));

                var updatedEntities = this.connection.GetRange<CompositeKeys>(
                    "WHERE Name = 'Other name'",
                    dialect: this.dialect);
                Assert.That(updatedEntities.Count(), Is.EqualTo(2));

                // Cleanup
                this.connection.DeleteAll<CompositeKeys>(dialect: this.dialect);
            }
        }

        private class DeleteIdAsync
            : DbConnectionAsyncExtensionsTests
        {
            public DeleteIdAsync(string dialectName)
                : base(dialectName)
            {
            }

            [Test]
            public async Task Deletes_the_entity_with_the_specified_id()
            {
                // Arrange
                var id = this.connection.Insert<int>(new User { Name = "Some name", Age = 10 }, dialect: this.dialect);

                // Act
                await this.connection.DeleteAsync<User>(id, dialect: this.dialect);

                // Assert
                Assert.That(this.connection.Find<User>(id, dialect: this.dialect), Is.Null);
            }

            [Test]
            public async Task Deletes_entity_with_string_key()
            {
                // Arrange
                this.connection.Insert(new KeyString { Name = "Some Name", Age = 10 }, dialect: this.dialect);

                // Act
                await this.connection.DeleteAsync<KeyString>("Some Name", dialect: this.dialect);
            }

            [Test]
            public async Task Deletes_entity_with_guid_key()
            {
                // Arrange
                var id = Guid.NewGuid();
                this.connection.Insert(new KeyGuid { Id = id, Name = "Some Name" }, dialect: this.dialect);

                // Act
                await this.connection.DeleteAsync<KeyGuid>(id, dialect: this.dialect);
            }

            [Test]
            public async Task Deletes_entity_with_composite_keys()
            {
                // Arrange
                var id = new { Key1 = 5, Key2 = 20 };
                var entity = new CompositeKeys { Key1 = 5, Key2 = 20, Name = "Some Name" };
                this.connection.Insert(entity, dialect: this.dialect);

                // Act
                await this.connection.DeleteAsync<CompositeKeys>(id, dialect: this.dialect);
            }
        }

        private class DeleteEntityAsync
            : DbConnectionAsyncExtensionsTests
        {
            public DeleteEntityAsync(string dialectName)
                : base(dialectName)
            {
            }

            [Test]
            public async Task Deletes_entity_with_matching_key()
            {
                // Arrange
                var id = this.connection.Insert<int>(new User { Name = "Some name", Age = 10 }, dialect: this.dialect);

                // Act
                var entity = this.connection.Find<User>(id, dialect: this.dialect);
                await this.connection.DeleteAsync(entity, dialect: this.dialect);

                // Assert
                Assert.That(this.connection.Find<User>(id, dialect: this.dialect), Is.Null);
            }

            [Test]
            public async Task Deletes_entity_with_composite_keys()
            {
                // Arrange
                var id = new { Key1 = 5, Key2 = 20 };
                var entity = new CompositeKeys { Key1 = 5, Key2 = 20, Name = "Some Name" };
                this.connection.Insert(entity, dialect: this.dialect);

                // Act
                await this.connection.DeleteAsync(entity, dialect: this.dialect);

                // Assert
                Assert.That(this.connection.Find<CompositeKeys>(id, dialect: this.dialect), Is.Null);
            }
        }

        private class DeleteRangeAsync
            : DbConnectionAsyncExtensionsTests
        {
            public DeleteRangeAsync(string dialectName)
                : base(dialectName)
            {
            }

            [TestCase(null)]
            [TestCase("")]
            [TestCase(" ")]
            [TestCase("HAVING Age = 10")]
            [TestCase("WHERE")]
            public void Throws_exception_if_conditions_does_not_contain_where_clause(string conditions)
            {
                // Act / Assert
                Assert.ThrowsAsync<ArgumentException>(
                    async () => await this.connection.DeleteRangeAsync<User>(conditions, dialect: this.dialect));
            }

            [TestCase("Where Age = 10")]
            [TestCase("where Age = 10")]
            [TestCase("WHERE Age = 10")]
            public void Allows_any_capitalization_of_where_clause(string conditions)
            {
                // Act / Assert
                Assert.DoesNotThrowAsync(
                    async () => await this.connection.DeleteRangeAsync<User>(conditions, dialect: this.dialect));
            }

            [Test]
            public async Task Deletes_all_matching_entities()
            {
                // Arrange
                this.connection.Insert(new User { Name = "Some Name 1", Age = 10 }, dialect: this.dialect);
                this.connection.Insert(new User { Name = "Some Name 2", Age = 10 }, dialect: this.dialect);
                this.connection.Insert(new User { Name = "Some Name 3", Age = 10 }, dialect: this.dialect);
                this.connection.Insert(new User { Name = "Some Name 4", Age = 11 }, dialect: this.dialect);

                // Act
                var result = await this.connection.DeleteRangeAsync<User>(
                    "WHERE Age = @Age",
                    new { Age = 10 },
                    dialect: this.dialect);

                // Assert
                Assert.AreEqual(3, result.NumRowsAffected);
                Assert.AreEqual(1, this.connection.Count<User>(dialect: this.dialect));
            }
        }

        private class DeleteAllAsync
            : DbConnectionAsyncExtensionsTests
        {
            public DeleteAllAsync(string dialectName)
                : base(dialectName)
            {
            }

            [Test]
            public async Task Deletes_all_entities()
            {
                // Arrange
                this.connection.Insert(new User { Name = "Some Name 1", Age = 10 }, dialect: this.dialect);
                this.connection.Insert(new User { Name = "Some Name 2", Age = 10 }, dialect: this.dialect);
                this.connection.Insert(new User { Name = "Some Name 3", Age = 10 }, dialect: this.dialect);
                this.connection.Insert(new User { Name = "Some Name 4", Age = 11 }, dialect: this.dialect);

                // Act
                var result = await this.connection.DeleteAllAsync<User>(dialect: this.dialect);

                // Assert
                Assert.AreEqual(4, result.NumRowsAffected);
                Assert.AreEqual(0, this.connection.Count<User>(dialect: this.dialect));
            }
        }
    }
}