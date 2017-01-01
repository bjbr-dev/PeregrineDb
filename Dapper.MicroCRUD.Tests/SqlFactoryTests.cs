// <copyright file="SqlFactoryTests.cs" company="Berkeleybross">
// Copyright (c) Berkeleybross. All rights reserved.
// </copyright>
namespace Dapper.MicroCRUD.Tests
{
    using Dapper.MicroCRUD.Tests.ExampleEntities;
    using Dapper.MicroCRUD.Tests.Utils;
    using NUnit.Framework;

    [TestFixture]
    public class SqlFactoryTests
    {
        private readonly Dialect dialect = Dialect.SqlServer2012;

        private class MakeCountStatement
            : SqlFactoryTests
        {
            [Test]
            public void Selects_from_given_table()
            {
                // Arrange
                var schema = this.dialect.User();

                // Act
                var sql = SqlFactory.MakeCountStatement(schema, null);

                // Assert
                var expected = @"SELECT COUNT(*)
FROM [Users]";

                Assert.That(sql, Is.EqualTo(expected).Using(SqlStringComparer.Instance));
            }

            [Test]
            public void Adds_conditions()
            {
                // Arrange
                var schema = this.dialect.User();

                // Act
                var sql = SqlFactory.MakeCountStatement(schema, "WHERE Foo IS NOT NULL");

                // Assert
                var expected = @"SELECT COUNT(*)
FROM [Users]
WHERE Foo IS NOT NULL";

                Assert.That(sql, Is.EqualTo(expected).Using(SqlStringComparer.Instance));
            }
        }

        private class MakeFindStatement
            : SqlFactoryTests
        {
            [Test]
            public void Selects_from_given_table()
            {
                // Arrange
                var schema = this.dialect.User();

                // Act
                var sql = SqlFactory.MakeFindStatement(schema);

                // Assert
                var expected = @"SELECT [Id], [Name], [Age]
FROM [Users]
WHERE [Id] = @Id";

                Assert.That(sql, Is.EqualTo(expected).Using(SqlStringComparer.Instance));
            }

            [Test]
            public void Uses_non_default_primary_key_name()
            {
                // Arrange
                var schema = this.dialect.KeyNotDefault();

                // Act
                var sql = SqlFactory.MakeFindStatement(schema);

                // Assert
                var expected = @"SELECT [Key], [Name]
FROM [KeyNotDefault]
WHERE [Key] = @Id";

                Assert.That(sql, Is.EqualTo(expected).Using(SqlStringComparer.Instance));
            }

            [Test]
            public void Adds_alias_when_primary_key_is_aliased()
            {
                // Arrange
                var schema = this.dialect.KeyAlias();

                // Act
                var sql = SqlFactory.MakeFindStatement(schema);

                // Assert
                var expected = @"SELECT [Key] AS [Id], [Name]
FROM [KeyAlias]
WHERE [Key] = @Id";

                Assert.That(sql, Is.EqualTo(expected).Using(SqlStringComparer.Instance));
            }

            [Test]
            public void Adds_alias_when_column_name_is_aliased()
            {
                // Arrange
                var schema = this.dialect.PropertyAlias();

                // Act
                var sql = SqlFactory.MakeFindStatement(schema);

                // Assert
                var expected = @"SELECT [Id], [YearsOld] AS [Age]
FROM [PropertyAlias]
WHERE [Id] = @Id";

                Assert.That(sql, Is.EqualTo(expected).Using(SqlStringComparer.Instance));
            }
        }

        private class MakeGetRangeStatement
            : SqlFactoryTests
        {
            [Test]
            public void Selects_from_given_table()
            {
                // Arrange
                var schema = this.dialect.User();

                // Act
                var sql = SqlFactory.MakeGetRangeStatement(schema, null);

                // Assert
                var expected = @"SELECT [Id], [Name], [Age]
FROM [Users]";

                Assert.That(sql, Is.EqualTo(expected).Using(SqlStringComparer.Instance));
            }

            [Test]
            public void Adds_conditions_clause()
            {
                // Arrange
                var schema = this.dialect.User();

                // Act
                var sql = SqlFactory.MakeGetRangeStatement(schema, "WHERE Age > @Age");

                // Assert
                var expected = @"SELECT [Id], [Name], [Age]
FROM [Users]
WHERE Age > @Age";

                Assert.That(sql, Is.EqualTo(expected).Using(SqlStringComparer.Instance));
            }

            [Test]
            public void Uses_non_default_primary_key_name()
            {
                // Arrange
                var schema = this.dialect.KeyNotDefault();

                // Act
                var sql = SqlFactory.MakeGetRangeStatement(schema, null);

                // Assert
                var expected = @"SELECT [Key], [Name]
FROM [KeyNotDefault]";

                Assert.That(sql, Is.EqualTo(expected).Using(SqlStringComparer.Instance));
            }

            [Test]
            public void Adds_alias_when_primary_key_is_aliased()
            {
                // Arrange
                var schema = this.dialect.KeyAlias();

                // Act
                var sql = SqlFactory.MakeGetRangeStatement(schema, null);

                // Assert
                var expected = @"SELECT [Key] AS [Id], [Name]
FROM [KeyAlias]";

                Assert.That(sql, Is.EqualTo(expected).Using(SqlStringComparer.Instance));
            }

            [Test]
            public void Adds_alias_when_column_name_is_aliased()
            {
                // Arrange
                var schema = this.dialect.PropertyAlias();

                // Act
                var sql = SqlFactory.MakeGetRangeStatement(schema, null);

                // Assert
                var expected = @"SELECT [Id], [YearsOld] AS [Age]
FROM [PropertyAlias]";

                Assert.That(sql, Is.EqualTo(expected).Using(SqlStringComparer.Instance));
            }
        }

        private class MakeInsertStatement
            : SqlFactoryTests
        {
            [Test]
            public void Inserts_into_given_table()
            {
                // Arrange
                var schema = this.dialect.User();

                // Act
                var sql = SqlFactory.MakeInsertStatement(schema);

                // Assert
                var expected = @"INSERT INTO [Users] ([Name], [Age])
VALUES (@Name, @Age);";

                Assert.That(sql, Is.EqualTo(expected).Using(SqlStringComparer.Instance));
            }

            [Test]
            public void Adds_primary_key_if_its_not_generated_by_database()
            {
                // Arrange
                var schema = this.dialect.KeyNotGenerated();

                // Act
                var sql = SqlFactory.MakeInsertStatement(schema);

                // Assert
                var expected = @"INSERT INTO [KeyNotGenerated] ([Id], [Name])
VALUES (@Id, @Name);";

                Assert.That(sql, Is.EqualTo(expected).Using(SqlStringComparer.Instance));
            }

            [Test]
            public void Does_not_include_computed_columns()
            {
                // Arrange
                var schema = this.dialect.PropertyComputed();

                // Act
                var sql = SqlFactory.MakeInsertStatement(schema);

                // Assert
                var expected = @"INSERT INTO [PropertyComputed] ([Name])
VALUES (@Name);";

                Assert.That(sql, Is.EqualTo(expected).Using(SqlStringComparer.Instance));
            }

            [Test]
            public void Does_not_include_generated_columns()
            {
                // Arrange
                var schema = this.dialect.PropertyGenerated();

                // Act
                var sql = SqlFactory.MakeInsertStatement(schema);

                // Assert
                var expected = @"INSERT INTO [PropertyGenerated] ([Name])
VALUES (@Name);";

                Assert.That(sql, Is.EqualTo(expected).Using(SqlStringComparer.Instance));
            }
        }

        private class MakeInsertReturningIdentityStatement
            : SqlFactoryTests
        {
            [Test]
            public void Inserts_into_given_table()
            {
                // Arrange
                var schema = this.dialect.User();

                // Act
                var sql = SqlFactory.MakeInsertReturningIdentityStatement(schema, Dialect.SqlServer2012);

                // Assert
                var expected = @"INSERT INTO [Users] ([Name], [Age])
VALUES (@Name, @Age);
SELECT CAST(SCOPE_IDENTITY() AS BIGINT) AS [id]";

                Assert.That(sql, Is.EqualTo(expected).Using(SqlStringComparer.Instance));
            }

            [Test]
            public void Adds_primary_key_if_its_not_generated_by_database()
            {
                // Arrange
                var schema = this.dialect.KeyNotGenerated();

                // Act
                var sql = SqlFactory.MakeInsertReturningIdentityStatement(schema, Dialect.SqlServer2012);

                // Assert
                var expected = @"INSERT INTO [KeyNotGenerated] ([Id], [Name])
VALUES (@Id, @Name);
SELECT CAST(SCOPE_IDENTITY() AS BIGINT) AS [id]";

                Assert.That(sql, Is.EqualTo(expected).Using(SqlStringComparer.Instance));
            }

            [Test]
            public void Does_not_include_computed_columns()
            {
                // Arrange
                var schema = this.dialect.PropertyComputed();

                // Act
                var sql = SqlFactory.MakeInsertReturningIdentityStatement(schema, Dialect.SqlServer2012);

                // Assert
                var expected = @"INSERT INTO [PropertyComputed] ([Name])
VALUES (@Name);
SELECT CAST(SCOPE_IDENTITY() AS BIGINT) AS [id]";

                Assert.That(sql, Is.EqualTo(expected).Using(SqlStringComparer.Instance));
            }

            [Test]
            public void Does_not_include_generated_columns()
            {
                // Arrange
                var schema = this.dialect.PropertyGenerated();

                // Act
                var sql = SqlFactory.MakeInsertReturningIdentityStatement(schema, Dialect.SqlServer2012);

                // Assert
                var expected = @"INSERT INTO [PropertyGenerated] ([Name])
VALUES (@Name);
SELECT CAST(SCOPE_IDENTITY() AS BIGINT) AS [id]";

                Assert.That(sql, Is.EqualTo(expected).Using(SqlStringComparer.Instance));
            }
        }

        private class MakeUpdateStatement
            : SqlFactoryTests
        {
            [Test]
            public void Updates_given_table()
            {
                // Arrange
                var schema = this.dialect.User();

                // Act
                var sql = SqlFactory.MakeUpdateStatement(schema);

                // Assert
                var expected = @"UPDATE [Users]
SET [Name] = @Name, [Age] = @Age
WHERE [Id] = @Id";

                Assert.That(sql, Is.EqualTo(expected).Using(SqlStringComparer.Instance));
            }

            [Test]
            public void Does_not_update_primary_key_even_if_its_not_auto_generated()
            {
                // Arrange
                var schema = this.dialect.KeyNotGenerated();

                // Act
                var sql = SqlFactory.MakeUpdateStatement(schema);

                // Assert
                var expected = @"UPDATE [KeyNotGenerated]
SET [Name] = @Name
WHERE [Id] = @Id";

                Assert.That(sql, Is.EqualTo(expected).Using(SqlStringComparer.Instance));
            }

            [Test]
            public void Uses_aliased_property_names()
            {
                // Arrange
                var schema = this.dialect.PropertyAlias();

                // Act
                var sql = SqlFactory.MakeUpdateStatement(schema);

                // Assert
                var expected = @"UPDATE [PropertyAlias]
SET [YearsOld] = @Age
WHERE [Id] = @Id";

                Assert.That(sql, Is.EqualTo(expected).Using(SqlStringComparer.Instance));
            }

            [Test]
            public void Uses_aliased_key_name()
            {
                // Arrange
                var schema = this.dialect.KeyAlias();

                // Act
                var sql = SqlFactory.MakeUpdateStatement(schema);

                // Assert
                var expected = @"UPDATE [KeyAlias]
SET [Name] = @Name
WHERE [Key] = @Id";

                Assert.That(sql, Is.EqualTo(expected).Using(SqlStringComparer.Instance));
            }

            [Test]
            public void Uses_non_default_key_name()
            {
                // Arrange
                var schema = this.dialect.KeyNotDefault();

                // Act
                var sql = SqlFactory.MakeUpdateStatement(schema);

                // Assert
                var expected = @"UPDATE [KeyNotDefault]
SET [Name] = @Name
WHERE [Key] = @Key";

                Assert.That(sql, Is.EqualTo(expected).Using(SqlStringComparer.Instance));
            }

            [Test]
            public void Does_not_include_computed_columns()
            {
                // Arrange
                var schema = this.dialect.PropertyComputed();

                // Act
                var sql = SqlFactory.MakeUpdateStatement(schema);

                // Assert
                var expected = @"UPDATE [PropertyComputed]
SET [Name] = @Name
WHERE [Id] = @Id";

                Assert.That(sql, Is.EqualTo(expected).Using(SqlStringComparer.Instance));
            }

            [Test]
            public void Includes_generated_columns()
            {
                // Arrange
                var schema = this.dialect.PropertyGenerated();

                // Act
                var sql = SqlFactory.MakeUpdateStatement(schema);

                // Assert
                var expected = @"UPDATE [PropertyGenerated]
SET [Name] = @Name, [Created] = @Created
WHERE [Id] = @Id";

                Assert.That(sql, Is.EqualTo(expected).Using(SqlStringComparer.Instance));
            }
        }

        private class MakeDeleteByPrimaryKeyStatement
            : SqlFactoryTests
        {
            [Test]
            public void Deletes_from_given_table()
            {
                // Arrange
                var schema = this.dialect.User();

                // Act
                var sql = SqlFactory.MakeDeleteByPrimaryKeyStatement(schema);

                // Assert
                var expected = @"DELETE FROM [Users]
WHERE [Id] = @Id";

                Assert.That(sql, Is.EqualTo(expected).Using(SqlStringComparer.Instance));
            }

            [Test]
            public void Uses_primary_key_even_if_its_not_auto_generated()
            {
                // Arrange
                var schema = this.dialect.KeyNotGenerated();

                // Act
                var sql = SqlFactory.MakeDeleteByPrimaryKeyStatement(schema);

                // Assert
                var expected = @"DELETE FROM [KeyNotGenerated]
WHERE [Id] = @Id";

                Assert.That(sql, Is.EqualTo(expected).Using(SqlStringComparer.Instance));
            }

            [Test]
            public void Uses_aliased_key_name()
            {
                // Arrange
                var schema = this.dialect.KeyAlias();

                // Act
                var sql = SqlFactory.MakeDeleteByPrimaryKeyStatement(schema);

                // Assert
                var expected = @"DELETE FROM [KeyAlias]
WHERE [Key] = @Id";

                Assert.That(sql, Is.EqualTo(expected).Using(SqlStringComparer.Instance));
            }

            [Test]
            public void Uses_non_default_key_name()
            {
                // Arrange
                var schema = this.dialect.KeyNotDefault();

                // Act
                var sql = SqlFactory.MakeDeleteByPrimaryKeyStatement(schema);

                // Assert
                var expected = @"DELETE FROM [KeyNotDefault]
WHERE [Key] = @Key";

                Assert.That(sql, Is.EqualTo(expected).Using(SqlStringComparer.Instance));
            }
        }

        private class MakeDeleteRangeStatement
            : SqlFactoryTests
        {
            [Test]
            public void Deletes_from_given_table()
            {
                // Arrange
                var schema = this.dialect.User();

                // Act
                var sql = SqlFactory.MakeDeleteRangeStatement(schema, "WHERE [Age] > 10");

                // Assert
                var expected = @"DELETE FROM [Users]
WHERE [Age] > 10";

                Assert.That(sql, Is.EqualTo(expected).Using(SqlStringComparer.Instance));
            }
        }
    }
}