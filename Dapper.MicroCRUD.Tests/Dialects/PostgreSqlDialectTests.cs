// <copyright file="PostgreSqlDialectTests.cs" company="Berkeleybross">
// Copyright (c) Berkeleybross. All rights reserved.
// </copyright>
namespace Dapper.MicroCRUD.Tests.Dialects
{
    using System;
    using System.Collections.Immutable;
    using Dapper.MicroCRUD.Dialects;
    using Dapper.MicroCRUD.Schema;
    using Dapper.MicroCRUD.Tests.ExampleEntities;
    using Dapper.MicroCRUD.Tests.Utils;
    using NUnit.Framework;

    [TestFixture]
    public class PostgreSqlDialectTests
    {
        private IDialect dialect;

        [SetUp]
        public void BaseSetUp()
        {
            this.dialect = Dialect.PostgreSql;
        }

        private class MakeCountStatement
            : PostgreSqlDialectTests
        {
            [Test]
            public void Selects_from_given_table()
            {
                // Arrange
                var schema = this.dialect.User();

                // Act
                var sql = this.dialect.MakeCountStatement(schema, null);

                // Assert
                var expected = @"SELECT COUNT(*)
FROM Users";

                Assert.That(sql, Is.EqualTo(expected).Using(SqlStringComparer.Instance));
            }

            [Test]
            public void Adds_conditions()
            {
                // Arrange
                var schema = this.dialect.User();

                // Act
                var sql = this.dialect.MakeCountStatement(schema, "WHERE Foo IS NOT NULL");

                // Assert
                var expected = @"SELECT COUNT(*)
FROM Users
WHERE Foo IS NOT NULL";

                Assert.That(sql, Is.EqualTo(expected).Using(SqlStringComparer.Instance));
            }
        }

        private class MakeFindStatement
            : PostgreSqlDialectTests
        {
            [Test]
            public void Selects_from_given_table()
            {
                // Arrange
                var schema = this.dialect.User();

                // Act
                var sql = this.dialect.MakeFindStatement(schema);

                // Assert
                var expected = @"SELECT Id, Name, Age
FROM Users
WHERE Id = @Id";

                Assert.That(sql, Is.EqualTo(expected).Using(SqlStringComparer.Instance));
            }

            [Test]
            public void Uses_non_default_primary_key_name()
            {
                // Arrange
                var schema = this.dialect.KeyNotDefault();

                // Act
                var sql = this.dialect.MakeFindStatement(schema);

                // Assert
                var expected = @"SELECT Key, Name
FROM KeyNotDefault
WHERE Key = @Key";

                Assert.That(sql, Is.EqualTo(expected).Using(SqlStringComparer.Instance));
            }

            [Test]
            public void Uses_each_key_in_composite_key()
            {
                // Arrange
                var schema = this.dialect.CompositeKeys();

                // Act
                var sql = this.dialect.MakeFindStatement(schema);

                // Assert
                var expected = @"SELECT Key1, Key2, Name
FROM CompositeKeys
WHERE Key1 = @Key1 AND Key2 = @Key2";

                Assert.That(sql, Is.EqualTo(expected).Using(SqlStringComparer.Instance));
            }

            [Test]
            public void Adds_alias_when_primary_key_is_aliased()
            {
                // Arrange
                var schema = this.dialect.KeyAlias();

                // Act
                var sql = this.dialect.MakeFindStatement(schema);

                // Assert
                var expected = @"SELECT Key AS Id, Name
FROM KeyAlias
WHERE Key = @Id";

                Assert.That(sql, Is.EqualTo(expected).Using(SqlStringComparer.Instance));
            }

            [Test]
            public void Adds_alias_when_column_name_is_aliased()
            {
                // Arrange
                var schema = this.dialect.PropertyAlias();

                // Act
                var sql = this.dialect.MakeFindStatement(schema);

                // Assert
                var expected = @"SELECT Id, YearsOld AS Age
FROM PropertyAlias
WHERE Id = @Id";

                Assert.That(sql, Is.EqualTo(expected).Using(SqlStringComparer.Instance));
            }
        }

        private class MakeGetRangeStatement
            : PostgreSqlDialectTests
        {
            [Test]
            public void Selects_from_given_table()
            {
                // Arrange
                var schema = this.dialect.User();

                // Act
                var sql = this.dialect.MakeGetRangeStatement(schema, null);

                // Assert
                var expected = @"SELECT Id, Name, Age
FROM Users";

                Assert.That(sql, Is.EqualTo(expected).Using(SqlStringComparer.Instance));
            }

            [Test]
            public void Adds_conditions_clause()
            {
                // Arrange
                var schema = this.dialect.User();

                // Act
                var sql = this.dialect.MakeGetRangeStatement(schema, "WHERE Age > @Age");

                // Assert
                var expected = @"SELECT Id, Name, Age
FROM Users
WHERE Age > @Age";

                Assert.That(sql, Is.EqualTo(expected).Using(SqlStringComparer.Instance));
            }

            [Test]
            public void Uses_non_default_primary_key_name()
            {
                // Arrange
                var schema = this.dialect.KeyNotDefault();

                // Act
                var sql = this.dialect.MakeGetRangeStatement(schema, null);

                // Assert
                var expected = @"SELECT Key, Name
FROM KeyNotDefault";

                Assert.That(sql, Is.EqualTo(expected).Using(SqlStringComparer.Instance));
            }

            [Test]
            public void Adds_alias_when_primary_key_is_aliased()
            {
                // Arrange
                var schema = this.dialect.KeyAlias();

                // Act
                var sql = this.dialect.MakeGetRangeStatement(schema, null);

                // Assert
                var expected = @"SELECT Key AS Id, Name
FROM KeyAlias";

                Assert.That(sql, Is.EqualTo(expected).Using(SqlStringComparer.Instance));
            }

            [Test]
            public void Adds_alias_when_column_name_is_aliased()
            {
                // Arrange
                var schema = this.dialect.PropertyAlias();

                // Act
                var sql = this.dialect.MakeGetRangeStatement(schema, null);

                // Assert
                var expected = @"SELECT Id, YearsOld AS Age
FROM PropertyAlias";

                Assert.That(sql, Is.EqualTo(expected).Using(SqlStringComparer.Instance));
            }
        }

        private class MakeGetPageStatement
            : PostgreSqlDialectTests
        {
            [TestCase(0)]
            [TestCase(-1)]
            public void Throws_exception_when_pageNumber_is_less_than_1(int pageNumber)
            {
                // Arrange
                var schema = this.dialect.User();

                // Act / Assert
                Assert.Throws<ArgumentException>(
                    () => this.dialect.MakeGetPageStatement(schema, this.dialect, pageNumber, 10, null, "Name"));
            }

            [TestCase(-1)]
            public void Throws_exception_when_itemsPerPage_is_less_than_0(int itemsPerPage)
            {
                // Arrange
                var schema = this.dialect.User();

                // Act / Assert
                Assert.Throws<ArgumentException>(
                    () => this.dialect.MakeGetPageStatement(schema, this.dialect, 1, itemsPerPage, null, "Name"));
            }

            [TestCase(null)]
            [TestCase("")]
            [TestCase(" ")]
            public void Throws_exception_when_order_by_is_empty(string orderBy)
            {
                // Arrange
                var schema = this.dialect.User();

                // Act / Assert
                Assert.Throws<ArgumentException>(
                    () => this.dialect.MakeGetPageStatement(schema, this.dialect, 1, 10, null, orderBy));
            }

            [Test]
            public void Selects_from_given_table()
            {
                // Arrange
                var schema = this.dialect.User();

                // Act
                var sql = this.dialect.MakeGetPageStatement(schema, this.dialect, 1, 10, null, "Name");

                // Assert
                var expected = @"SELECT Id, Name, Age
FROM Users
ORDER BY Name
LIMIT 10 OFFSET 0";

                Assert.That(sql, Is.EqualTo(expected).Using(SqlStringComparer.Instance));
            }

            [Test]
            public void Adds_conditions_clause()
            {
                // Arrange
                var schema = this.dialect.User();

                // Act
                var sql = this.dialect.MakeGetPageStatement(schema, this.dialect, 1, 10, "WHERE Name LIKE 'Foo%'", "Name");

                // Assert
                var expected = @"SELECT Id, Name, Age
FROM Users
WHERE Name LIKE 'Foo%'
ORDER BY Name
LIMIT 10 OFFSET 0";

                Assert.That(sql, Is.EqualTo(expected).Using(SqlStringComparer.Instance));
            }

            [Test]
            public void Adds_alias_when_column_name_is_aliased()
            {
                // Arrange
                var schema = this.dialect.PropertyAlias();

                // Act
                var sql = this.dialect.MakeGetPageStatement(schema, this.dialect, 1, 10, null, "Name");

                // Assert
                var expected = @"SELECT Id, YearsOld AS Age
FROM PropertyAlias
ORDER BY Name
LIMIT 10 OFFSET 0";

                Assert.That(sql, Is.EqualTo(expected).Using(SqlStringComparer.Instance));
            }

            [Test]
            public void Selects_second_page()
            {
                // Arrange
                var schema = this.dialect.User();

                // Act
                var sql = this.dialect.MakeGetPageStatement(schema, this.dialect, 2, 10, null, "Name");

                // Assert
                var expected = @"SELECT Id, Name, Age
FROM Users
ORDER BY Name
LIMIT 10 OFFSET 10";

                Assert.That(sql, Is.EqualTo(expected).Using(SqlStringComparer.Instance));
            }

            [Test]
            public void Selects_appropriate_number_of_rows()
            {
                // Arrange
                var schema = this.dialect.User();

                // Act
                var sql = this.dialect.MakeGetPageStatement(schema, this.dialect, 2, 5, null, "Name");

                // Assert
                var expected = @"SELECT Id, Name, Age
FROM Users
ORDER BY Name
LIMIT 5 OFFSET 5";

                Assert.That(sql, Is.EqualTo(expected).Using(SqlStringComparer.Instance));
            }

            [Test]
            public void Allows_itemsPerPage_to_be_zero()
            {
                // Arrange
                var schema = this.dialect.User();

                // Act
                var sql = this.dialect.MakeGetPageStatement(schema, this.dialect, 2, 0, null, "Name");

                // Assert
                var expected = @"SELECT Id, Name, Age
FROM Users
ORDER BY Name
LIMIT 0 OFFSET 0";

                Assert.That(sql, Is.EqualTo(expected).Using(SqlStringComparer.Instance));
            }
        }

        private class MakeInsertStatement
            : PostgreSqlDialectTests
        {
            [Test]
            public void Inserts_into_given_table()
            {
                // Arrange
                var schema = this.dialect.User();

                // Act
                var sql = this.dialect.MakeInsertStatement(schema);

                // Assert
                var expected = @"INSERT INTO Users (Name, Age)
VALUES (@Name, @Age);";

                Assert.That(sql, Is.EqualTo(expected).Using(SqlStringComparer.Instance));
            }

            [Test]
            public void Adds_primary_key_if_its_not_generated_by_database()
            {
                // Arrange
                var schema = this.dialect.KeyNotGenerated();

                // Act
                var sql = this.dialect.MakeInsertStatement(schema);

                // Assert
                var expected = @"INSERT INTO KeyNotGenerated (Id, Name)
VALUES (@Id, @Name);";

                Assert.That(sql, Is.EqualTo(expected).Using(SqlStringComparer.Instance));
            }

            [Test]
            public void Does_not_include_computed_columns()
            {
                // Arrange
                var schema = this.dialect.PropertyComputed();

                // Act
                var sql = this.dialect.MakeInsertStatement(schema);

                // Assert
                var expected = @"INSERT INTO PropertyComputed (Name)
VALUES (@Name);";

                Assert.That(sql, Is.EqualTo(expected).Using(SqlStringComparer.Instance));
            }

            [Test]
            public void Does_not_include_generated_columns()
            {
                // Arrange
                var schema = this.dialect.PropertyGenerated();

                // Act
                var sql = this.dialect.MakeInsertStatement(schema);

                // Assert
                var expected = @"INSERT INTO PropertyGenerated (Name)
VALUES (@Name);";

                Assert.That(sql, Is.EqualTo(expected).Using(SqlStringComparer.Instance));
            }
        }

        private class MakeInsertReturningIdentityStatement
            : PostgreSqlDialectTests
        {
            [Test]
            public void Inserts_into_given_table()
            {
                // Arrange
                var schema = this.dialect.User();

                // Act
                var sql = this.dialect.MakeInsertReturningIdentityStatement(schema);

                // Assert
                var expected = @"INSERT INTO Users (Name, Age)
VALUES (@Name, @Age)
RETURNING Id";

                Assert.That(sql, Is.EqualTo(expected).Using(SqlStringComparer.Instance));
            }

            [Test]
            public void Adds_primary_key_if_its_not_generated_by_database()
            {
                // Arrange
                var schema = this.dialect.KeyNotGenerated();

                // Act
                var sql = this.dialect.MakeInsertReturningIdentityStatement(schema);

                // Assert
                var expected = @"INSERT INTO KeyNotGenerated (Id, Name)
VALUES (@Id, @Name)
RETURNING Id";

                Assert.That(sql, Is.EqualTo(expected).Using(SqlStringComparer.Instance));
            }

            [Test]
            public void Does_not_include_computed_columns()
            {
                // Arrange
                var schema = this.dialect.PropertyComputed();

                // Act
                var sql = this.dialect.MakeInsertReturningIdentityStatement(schema);

                // Assert
                var expected = @"INSERT INTO PropertyComputed (Name)
VALUES (@Name)
RETURNING Id";

                Assert.That(sql, Is.EqualTo(expected).Using(SqlStringComparer.Instance));
            }

            [Test]
            public void Does_not_include_generated_columns()
            {
                // Arrange
                var schema = this.dialect.PropertyGenerated();

                // Act
                var sql = this.dialect.MakeInsertReturningIdentityStatement(schema);

                // Assert
                var expected = @"INSERT INTO PropertyGenerated (Name)
VALUES (@Name)
RETURNING Id";

                Assert.That(sql, Is.EqualTo(expected).Using(SqlStringComparer.Instance));
            }
        }

        private class MakeUpdateStatement
            : PostgreSqlDialectTests
        {
            [Test]
            public void Updates_given_table()
            {
                // Arrange
                var schema = this.dialect.User();

                // Act
                var sql = this.dialect.MakeUpdateStatement(schema);

                // Assert
                var expected = @"UPDATE Users
SET Name = @Name, Age = @Age
WHERE Id = @Id";

                Assert.That(sql, Is.EqualTo(expected).Using(SqlStringComparer.Instance));
            }

            [Test]
            public void Uses_each_key_in_composite_key()
            {
                // Arrange
                var schema = this.dialect.CompositeKeys();

                // Act
                var sql = this.dialect.MakeUpdateStatement(schema);

                // Assert
                var expected = @"UPDATE CompositeKeys
SET Name = @Name
WHERE Key1 = @Key1 AND Key2 = @Key2";

                Assert.That(sql, Is.EqualTo(expected).Using(SqlStringComparer.Instance));
            }

            [Test]
            public void Does_not_update_primary_key_even_if_its_not_auto_generated()
            {
                // Arrange
                var schema = this.dialect.KeyNotGenerated();

                // Act
                var sql = this.dialect.MakeUpdateStatement(schema);

                // Assert
                var expected = @"UPDATE KeyNotGenerated
SET Name = @Name
WHERE Id = @Id";

                Assert.That(sql, Is.EqualTo(expected).Using(SqlStringComparer.Instance));
            }

            [Test]
            public void Uses_aliased_property_names()
            {
                // Arrange
                var schema = this.dialect.PropertyAlias();

                // Act
                var sql = this.dialect.MakeUpdateStatement(schema);

                // Assert
                var expected = @"UPDATE PropertyAlias
SET YearsOld = @Age
WHERE Id = @Id";

                Assert.That(sql, Is.EqualTo(expected).Using(SqlStringComparer.Instance));
            }

            [Test]
            public void Uses_aliased_key_name()
            {
                // Arrange
                var schema = this.dialect.KeyAlias();

                // Act
                var sql = this.dialect.MakeUpdateStatement(schema);

                // Assert
                var expected = @"UPDATE KeyAlias
SET Name = @Name
WHERE Key = @Id";

                Assert.That(sql, Is.EqualTo(expected).Using(SqlStringComparer.Instance));
            }

            [Test]
            public void Uses_non_default_key_name()
            {
                // Arrange
                var schema = this.dialect.KeyNotDefault();

                // Act
                var sql = this.dialect.MakeUpdateStatement(schema);

                // Assert
                var expected = @"UPDATE KeyNotDefault
SET Name = @Name
WHERE Key = @Key";

                Assert.That(sql, Is.EqualTo(expected).Using(SqlStringComparer.Instance));
            }

            [Test]
            public void Does_not_include_computed_columns()
            {
                // Arrange
                var schema = this.dialect.PropertyComputed();

                // Act
                var sql = this.dialect.MakeUpdateStatement(schema);

                // Assert
                var expected = @"UPDATE PropertyComputed
SET Name = @Name
WHERE Id = @Id";

                Assert.That(sql, Is.EqualTo(expected).Using(SqlStringComparer.Instance));
            }

            [Test]
            public void Includes_generated_columns()
            {
                // Arrange
                var schema = this.dialect.PropertyGenerated();

                // Act
                var sql = this.dialect.MakeUpdateStatement(schema);

                // Assert
                var expected = @"UPDATE PropertyGenerated
SET Name = @Name, Created = @Created
WHERE Id = @Id";

                Assert.That(sql, Is.EqualTo(expected).Using(SqlStringComparer.Instance));
            }
        }

        private class MakeDeleteByPrimaryKeyStatement
            : PostgreSqlDialectTests
        {
            [Test]
            public void Deletes_from_given_table()
            {
                // Arrange
                var schema = this.dialect.User();

                // Act
                var sql = this.dialect.MakeDeleteByPrimaryKeyStatement(schema);

                // Assert
                var expected = @"DELETE FROM Users
WHERE Id = @Id";

                Assert.That(sql, Is.EqualTo(expected).Using(SqlStringComparer.Instance));
            }

            [Test]
            public void Uses_each_key_in_composite_key()
            {
                // Arrange
                var schema = this.dialect.CompositeKeys();

                // Act
                var sql = this.dialect.MakeDeleteByPrimaryKeyStatement(schema);

                // Assert
                var expected = @"DELETE FROM CompositeKeys
WHERE Key1 = @Key1 AND Key2 = @Key2";

                Assert.That(sql, Is.EqualTo(expected).Using(SqlStringComparer.Instance));
            }

            [Test]
            public void Uses_primary_key_even_if_its_not_auto_generated()
            {
                // Arrange
                var schema = this.dialect.KeyNotGenerated();

                // Act
                var sql = this.dialect.MakeDeleteByPrimaryKeyStatement(schema);

                // Assert
                var expected = @"DELETE FROM KeyNotGenerated
WHERE Id = @Id";

                Assert.That(sql, Is.EqualTo(expected).Using(SqlStringComparer.Instance));
            }

            [Test]
            public void Uses_aliased_key_name()
            {
                // Arrange
                var schema = this.dialect.KeyAlias();

                // Act
                var sql = this.dialect.MakeDeleteByPrimaryKeyStatement(schema);

                // Assert
                var expected = @"DELETE FROM KeyAlias
WHERE Key = @Id";

                Assert.That(sql, Is.EqualTo(expected).Using(SqlStringComparer.Instance));
            }

            [Test]
            public void Uses_non_default_key_name()
            {
                // Arrange
                var schema = this.dialect.KeyNotDefault();

                // Act
                var sql = this.dialect.MakeDeleteByPrimaryKeyStatement(schema);

                // Assert
                var expected = @"DELETE FROM KeyNotDefault
WHERE Key = @Key";

                Assert.That(sql, Is.EqualTo(expected).Using(SqlStringComparer.Instance));
            }
        }

        private class MakeDeleteRangeStatement
            : PostgreSqlDialectTests
        {
            [Test]
            public void Deletes_from_given_table()
            {
                // Arrange
                var schema = this.dialect.User();

                // Act
                var sql = this.dialect.MakeDeleteRangeStatement(schema, "WHERE Age > 10");

                // Assert
                var expected = @"DELETE FROM Users
WHERE Age > 10";

                Assert.That(sql, Is.EqualTo(expected).Using(SqlStringComparer.Instance));
            }
        }

        private class MakeWhereClause
            : PostgreSqlDialectTests
        {
            [Test]
            public void Returns_empty_string_for_empty_conditions_object()
            {
                // Arrange
                var conditions = new { };
                var schema = this.GetConditionsSchema<User>(conditions);

                // Act
                var sql = this.dialect.MakeWhereClause(schema, conditions);

                // Assert
                var expected = string.Empty;
                Assert.That(sql, Is.EqualTo(expected).Using(SqlStringComparer.Instance));
            }

            [Test]
            public void Includes_column_in_where_clause()
            {
                // Arrange
                var conditions = new { Name = "Bobby" };
                var schema = this.GetConditionsSchema<User>(conditions);

                // Act
                var sql = this.dialect.MakeWhereClause(schema, conditions);

                // Assert
                var expected = @"WHERE Name = @Name";

                Assert.That(sql, Is.EqualTo(expected).Using(SqlStringComparer.Instance));
            }

            [Test]
            public void Includes_all_columns_in_where_clause()
            {
                // Arrange
                var conditions = new { Name = "Bobby", Age = 5 };
                var schema = this.GetConditionsSchema<User>(conditions);

                // Act
                var sql = this.dialect.MakeWhereClause(schema, conditions);

                // Assert
                var expected = @"WHERE Name = @Name AND Age = @Age";

                Assert.That(sql, Is.EqualTo(expected).Using(SqlStringComparer.Instance));
            }

            [Test]
            public void Checks_for_null_when_condition_value_is_null()
            {
                // Arrange
                var conditions = new { Name = (string)null };
                var schema = this.GetConditionsSchema<User>(conditions);

                // Act
                var sql = this.dialect.MakeWhereClause(schema, conditions);

                // Assert
                var expected = @"WHERE Name IS NULL";

                Assert.That(sql, Is.EqualTo(expected).Using(SqlStringComparer.Instance));
            }

            private ImmutableArray<ConditionColumnSchema> GetConditionsSchema<TEntity>(object value)
            {
                var tableSchema = TableSchemaFactory.GetTableSchema(typeof(TEntity), this.dialect);
                return TableSchemaFactory.GetConditionsSchema(typeof(TEntity), tableSchema, value.GetType(), this.dialect);
            }
        }
    }
}