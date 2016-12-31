// <copyright file="DbConnectionExtensionsTests.cs" company="Berkeleybross">
//   Copyright (c) Berkeleybross. All rights reserved.
// </copyright>
namespace Dapper.MicroCRUD.Tests
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Linq;
    using Dapper.MicroCRUD.Tests.ExampleEntities;
    using Dapper.MicroCRUD.Tests.Utils;
    using NUnit.Framework;

    [Parallelizable(ParallelScope.None)]
    [TestFixtureSource(nameof(TestDialects))]
    [TestFixture]
    public class DbConnectionExtensionsTests
    {
        private readonly Dialect dialect;
        private IDbConnection connection;

        public DbConnectionExtensionsTests(Dialect dialect)
        {
            this.dialect = dialect;
        }

        public static IEnumerable<Dialect> TestDialects => new[] { Dialect.SqlServer2012, Dialect.PostgreSql };

        [OneTimeSetUp]
        public void OneTimeSetup()
        {
            switch (this.dialect.Name)
            {
                case nameof(Dialect.SqlServer2012):
                    this.connection = BlankDatabaseFactory.CreateSqlServer2012Database();
                    break;
                case nameof(Dialect.PostgreSql):
                    this.connection = BlankDatabaseFactory.CreatePostgreSqlDatabase();
                    break;
                default:
                    throw new InvalidOperationException();
            }

            this.connection.Open();
        }

        [OneTimeTearDown]
        public void OneTimeTearDown()
        {
            if (this.connection == null)
            {
                return;
            }

            var connectionString = this.connection.ConnectionString;
            this.connection.Dispose();

            switch (this.dialect.Name)
            {
                case nameof(Dialect.SqlServer2012):
                    BlankDatabaseFactory.DropSqlServer2012Database(connectionString);
                    break;
                case nameof(Dialect.PostgreSql):
                    BlankDatabaseFactory.DropPostgresDatabase(connectionString);
                    break;
                default:
                    throw new InvalidOperationException();
            }

            this.connection = null;
        }

        private class Find
            : DbConnectionExtensionsTests
        {
            public Find(Dialect dialect)
                : base(dialect)
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
                this.connection.Execute("INSERT INTO KeyString (Name, Age) VALUES ('Some Name', 42)");

                // Act
                var entity = this.connection.Find<KeyString>("Some Name", dialect: this.dialect);

                // Assert
                Assert.That(entity.Age, Is.EqualTo(42));

                // Cleanup
                this.connection.Execute("DELETE FROM KeyString");
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
        }

        private class GetRange
            : DbConnectionExtensionsTests
        {
            public GetRange(Dialect dialect)
                : base(dialect)
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
            public GetAll(Dialect dialect)
                : base(dialect)
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
            public InsertAndReturnKey(Dialect dialect)
                : base(dialect)
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
            public Update(Dialect dialect)
                : base(dialect)
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
            public DeleteId(Dialect dialect)
                : base(dialect)
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
                this.connection.Execute("INSERT INTO KeyString (Name, Age) VALUES ('Some name', 10)");

                // Act
                var result = this.connection.Delete<KeyString>("Some name", dialect: this.dialect);

                // Assert
                Assert.That(result, Is.EqualTo(1));
            }
        }

        private class DeleteEntity
            : DbConnectionExtensionsTests
        {
            public DeleteEntity(Dialect dialect)
                : base(dialect)
            {
            }

            [Test]
            public void TestDeleteByObject()
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