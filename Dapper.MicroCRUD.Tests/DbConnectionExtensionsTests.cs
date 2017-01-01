// <copyright file="DbConnectionExtensionsTests.cs" company="Berkeleybross">
//   Copyright (c) Berkeleybross. All rights reserved.
// </copyright>
namespace Dapper.MicroCRUD.Tests
{
    using System;
    using System.Data;
    using System.Linq;
    using Dapper.MicroCRUD.Tests.ExampleEntities;
    using Dapper.MicroCRUD.Tests.Utils;
    using NCrunch.Framework;
    using NUnit.Framework;

    [ExclusivelyUses("Database")]
    [Parallelizable(ParallelScope.None)]
    [TestFixtureSource(typeof(BlankDatabaseFactory), nameof(BlankDatabaseFactory.PossibleDialects))]
    public class DbConnectionExtensionsTests
    {
        private readonly string dialectName;

        private IDbConnection connection;
        private Dialect dialect;
        private BlankDatabase database;

        public DbConnectionExtensionsTests(string dialectName)
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

        private class Count
            : DbConnectionExtensionsTests
        {
            public Count(string dialectName)
                : base(dialectName)
            {
            }

            [Test]
            public void Counts_entities()
            {
                // Arrange
                this.connection.Insert(new User { Name = "Some Name 1", Age = 10 }, dialect: this.dialect);
                this.connection.Insert(new User { Name = "Some Name 2", Age = 10 }, dialect: this.dialect);
                this.connection.Insert(new User { Name = "Some Name 3", Age = 10 }, dialect: this.dialect);
                this.connection.Insert(new User { Name = "Some Name 4", Age = 11 }, dialect: this.dialect);

                // Act
                var result = this.connection.Count<User>(dialect: this.dialect);

                // Assert
                Assert.AreEqual(4, result);

                // Cleanup
                this.connection.Execute("DELETE FROM Users");
            }

            [Test]
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
                Assert.AreEqual(3, result);

                // Cleanup
                this.connection.Execute("DELETE FROM Users");
            }

            [Test]
            public void Counts_entities_in_alternate_schema()
            {
                this.connection.Insert<int>(new SchemaOther { Name = "Some Name" }, dialect: this.dialect);
                this.connection.Insert<int>(new SchemaOther { Name = "Some Name" }, dialect: this.dialect);
                this.connection.Insert<int>(new SchemaOther { Name = "Some Name" }, dialect: this.dialect);
                this.connection.Insert<int>(new SchemaOther { Name = "Some Name" }, dialect: this.dialect);

                // Act
                var result = this.connection.Count<SchemaOther>(dialect: this.dialect);

                // Assert
                Assert.AreEqual(4, result);

                // Cleanup
                this.connection.Execute("DELETE FROM Other.SchemaOther");
            }
        }

        private class Find
            : DbConnectionExtensionsTests
        {
            public Find(string dialectName)
                : base(dialectName)
            {
            }

            [Test]
            public void Throws_exception_when_entity_has_no_key()
            {
                // Arrange
                this.connection.Execute("INSERT INTO NoKey (Name, Age) VALUES ('Some Name', 1)");

                // Act
                Assert.Throws<InvalidPrimaryKeyException>(
                    () => this.connection.Find<NoKey>("Some Name", dialect: this.dialect));
            }

            [Test]
            public void Throws_exception_when_entity_has_composite_keys()
            {
                // Arrange
                this.connection.Execute("INSERT INTO CompositeKeys (Key1, Key2) VALUES (1, 1)");

                // Act
                Assert.Throws<InvalidPrimaryKeyException>(
                    () => this.connection.Find<CompositeKeys>(5, dialect: this.dialect));
            }

            [Test]
            public void Finds_entity_by_Int32_primary_key()
            {
                // Arrange
                var id = this.connection.Insert<int>(new KeyInt32 { Name = "Some Name" }, dialect: this.dialect);

                // Act
                var entity = this.connection.Find<KeyInt32>(id, dialect: this.dialect);

                // Assert
                Assert.That(entity.Name, Is.EqualTo("Some Name"));

                // Cleanup
                this.connection.Delete<KeyInt32>(id, dialect: this.dialect);
            }

            [Test]
            public void Finds_entity_by_Int64_primary_key()
            {
                // Arrange
                var id = this.connection.Insert<long>(new KeyInt64 { Name = "Some Name" }, dialect: this.dialect);

                // Act
                var user = this.connection.Find<KeyInt64>(id, dialect: this.dialect);

                // Assert
                Assert.That(user.Name, Is.EqualTo("Some Name"));

                // Cleanup
                this.connection.Delete<KeyInt64>(id, dialect: this.dialect);
            }

            [Test]
            public void Finds_entity_by_string_primary_key()
            {
                // Arrange
                this.connection.Insert(new KeyString { Name = "Some Name", Age = 42 }, dialect: this.dialect);

                // Act
                var entity = this.connection.Find<KeyString>("Some Name", dialect: this.dialect);

                // Assert
                Assert.That(entity.Age, Is.EqualTo(42));

                // Cleanup
                this.connection.Delete(entity, dialect: this.dialect);
            }

            [Test]
            public void Finds_entity_by_guid_primary_key()
            {
                // Arrange
                var id = Guid.NewGuid();
                this.connection.Insert(new KeyGuid { Id = id, Name = "Some Name" }, dialect: this.dialect);

                // Act
                var entity = this.connection.Find<KeyGuid>(id, dialect: this.dialect);

                // Assert
                Assert.That(entity.Name, Is.EqualTo("Some Name"));

                // Cleanup
                this.connection.Delete(entity, dialect: this.dialect);
            }

            [Test]
            public void Finds_entities_in_alternate_schema()
            {
                // Arrange
                var id = this.connection.Insert<int>(new SchemaOther { Name = "Some Name" }, dialect: this.dialect);

                // Act
                var entity = this.connection.Find<SchemaOther>(id, dialect: this.dialect);

                // Assert
                Assert.That(entity.Name, Is.EqualTo("Some Name"));

                // Cleanup
                this.connection.Delete<SchemaOther>(id, dialect: this.dialect);
            }

            [Test]
            public void Finds_entities_with_enum_property()
            {
                // Arrange
                var id = this.connection.Insert<int>(
                    new PropertyEnum { FavoriteColor = PropertyEnum.Color.Green },
                    dialect: this.dialect);

                // Act
                var entity = this.connection.Find<PropertyEnum>(id, dialect: this.dialect);

                // Assert
                Assert.That(entity.FavoriteColor, Is.EqualTo(PropertyEnum.Color.Green));

                // Cleanup
                this.connection.Delete<PropertyEnum>(id, dialect: this.dialect);
            }

            [Test]
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
        }

        private class GetRange
            : DbConnectionExtensionsTests
        {
            public GetRange(string dialectName)
                : base(dialectName)
            {
            }

            [Test]
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
                Assert.That(users.Count(), Is.EqualTo(3));

                // Cleanup
                this.connection.Execute("DELETE FROM Users");
            }

            [Test]
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
                Assert.That(users.Count(), Is.EqualTo(4));

                // Cleanup
                this.connection.Execute("DELETE FROM Users");
            }
        }

        private class GetAll
            : DbConnectionExtensionsTests
        {
            public GetAll(string dialectName)
                : base(dialectName)
            {
            }

            [Test]
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
                Assert.That(users.Count(), Is.EqualTo(4));

                // Cleanup
                this.connection.Execute("DELETE FROM Users");
            }
        }

        private class InsertAndReturnKey
            : DbConnectionExtensionsTests
        {
            public InsertAndReturnKey(string dialectName)
                : base(dialectName)
            {
            }

            [Test]
            public void Throws_exception_when_entity_has_no_key()
            {
                // Act
                Assert.Throws<InvalidPrimaryKeyException>(
                    () => this.connection.Insert<int>(new NoKey(), dialect: this.dialect));
            }

            [Test]
            public void Throws_exception_when_entity_has_composite_keys()
            {
                // Act
                Assert.Throws<InvalidPrimaryKeyException>(
                    () => this.connection.Insert<int>(new CompositeKeys(), dialect: this.dialect));
            }

            [Test]
            public void Throws_exception_for_string_keys()
            {
                // Arrange
                var entity = new KeyString { Name = "Some Name", Age = 10 };

                // Act / Assert
                Assert.Throws<InvalidPrimaryKeyException>(
                    () => this.connection.Insert<string>(entity, dialect: this.dialect));
            }

            [Test]
            public void Throws_exception_for_guid_keys()
            {
                // Arrange
                var entity = new KeyGuid { Id = Guid.NewGuid(), Name = "Some Name" };

                // Act / Assert
                Assert.Throws<InvalidPrimaryKeyException>(
                    () => this.connection.Insert<Guid>(entity, dialect: this.dialect));
            }

            [Test]
            public void Inserts_entity_with_int32_primary_key()
            {
                // Act
                var id = this.connection.Insert<int>(new KeyInt32 { Name = "Some Name" }, dialect: this.dialect);

                // Assert
                Assert.That(id, Is.GreaterThan(0));

                // Cleanup
                this.connection.Delete<KeyInt32>(id, dialect: this.dialect);
            }

            [Test]
            public void Inserts_entity_with_int64_primary_key()
            {
                // Act
                var id = this.connection.Insert<int>(new KeyInt64 { Name = "Some Name" }, dialect: this.dialect);

                // Assert
                Assert.That(id, Is.GreaterThan(0));

                // Cleanup
                this.connection.Delete<KeyInt64>(id, dialect: this.dialect);
            }

            [Test]
            public void Uses_key_attribute_to_determine_key()
            {
                // Act
                var id = this.connection.Insert<int>(new KeyAlias { Name = "Some Name" }, dialect: this.dialect);

                // Assert
                Assert.That(id, Is.GreaterThan(0));

                // Cleanup
                this.connection.Delete<KeyAlias>(id, dialect: this.dialect);
            }

            [Test]
            public void Inserts_into_other_schemas()
            {
                // Act
                var id = this.connection.Insert<int>(new SchemaOther { Name = "Some name" }, dialect: this.dialect);

                // Assert
                Assert.That(id, Is.GreaterThan(0));

                // Cleanup
                this.connection.Delete<SchemaOther>(id, dialect: this.dialect);
            }
        }

        private class Update
            : DbConnectionExtensionsTests
        {
            public Update(string dialectName)
                : base(dialectName)
            {
            }

            [Test]
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
                Assert.That(updatedEntity.Name, Is.EqualTo("Other name"));

                // Cleanup
                this.connection.Delete<User>(id, dialect: this.dialect);
            }
        }

        private class DeleteId
            : DbConnectionExtensionsTests
        {
            public DeleteId(string dialectName)
                : base(dialectName)
            {
            }

            [Test]
            public void Deletes_the_entity_with_the_specified_id()
            {
                // Arrange
                var id = this.connection.Insert<int>(new User { Name = "Some name", Age = 10 }, dialect: this.dialect);

                // Act
                this.connection.Delete<User>(id, dialect: this.dialect);

                // Assert
                Assert.That(this.connection.Find<User>(id, dialect: this.dialect), Is.Null);
            }

            [Test]
            public void Deletes_entity_with_string_key()
            {
                // Arrange
                this.connection.Insert(new KeyString { Name = "Some Name", Age = 10 }, dialect: this.dialect);

                // Act
                var result = this.connection.Delete<KeyString>("Some Name", dialect: this.dialect);

                // Assert
                Assert.That(result, Is.EqualTo(1));
            }

            [Test]
            public void Deletes_entity_with_guid_key()
            {
                // Arrange
                var id = Guid.NewGuid();
                this.connection.Insert(new KeyGuid { Id = id, Name = "Some Name" }, dialect: this.dialect);

                // Act
                var result = this.connection.Delete<KeyGuid>(id, dialect: this.dialect);

                // Assert
                Assert.That(result, Is.EqualTo(1));
            }
        }

        private class DeleteEntity
            : DbConnectionExtensionsTests
        {
            public DeleteEntity(string dialectName)
                : base(dialectName)
            {
            }

            [Test]
            public void Deletes_entity_with_matching_key()
            {
                // Arrange
                var id = this.connection.Insert<int>(new User { Name = "Some name", Age = 10 }, dialect: this.dialect);

                // Act
                var entity = this.connection.Find<User>(id, dialect: this.dialect);
                this.connection.Delete(entity, dialect: this.dialect);

                // Assert
                Assert.That(this.connection.Find<User>(id, dialect: this.dialect), Is.Null);
            }
        }
    }
}