namespace PeregrineDb.Tests.Dialects
{
    using System;
    using System.Collections.Immutable;
    using System.Data;
    using System.Diagnostics.CodeAnalysis;
    using Moq;
    using Pagination;
    using PeregrineDb;
    using PeregrineDb.Dialects;
    using PeregrineDb.Schema;
    using PeregrineDb.Tests.ExampleEntities;
    using PeregrineDb.Tests.Utils;
    using Xunit;

    [SuppressMessage("ReSharper", "ConvertToConstant.Local")]
    public class SqlServer2012DialectTests
    {
        private PeregrineConfig config = PeregrineConfig.SqlServer2012;

        public class MakeCountStatement
            : SqlServer2012DialectTests
        {
            [Fact]
            public void Selects_from_given_table()
            {
                // Arrange
                var schema = this.config.User();

                // Act
                var sql = this.config.Dialect.MakeCountStatement(schema, null);

                // Assert
                FormattableString expected = $@"
SELECT COUNT(*)
FROM [Users]";

                Assert.Equal(expected, sql, SqlStringComparer.Instance);
            }

            [Fact]
            public void Adds_conditions()
            {
                // Arrange
                var schema = this.config.User();

                // Act
                var sql = this.config.Dialect.MakeCountStatement(schema, $"WHERE Foo IS NOT NULL");

                // Assert
                FormattableString expected = $@"
SELECT COUNT(*)
FROM [Users]
WHERE Foo IS NOT NULL";

                Assert.Equal(expected, sql, SqlStringComparer.Instance);
            }
        }

        public class MakeFindStatement
            : SqlServer2012DialectTests
        {
            [Fact]
            public void Selects_from_given_table()
            {
                // Arrange
                var schema = this.config.User();

                // Act
                var sql = this.config.Dialect.MakeFindStatement(schema, 5);

                // Assert
                var expected = new SqlString(@"
SELECT [Id], [Name], [Age]
FROM [Users]
WHERE [Id] = {0}", 5, null, null);

                Assert.Equal(expected, sql, SqlStringComparer.Instance);
            }

            [Fact]
            public void Uses_non_default_primary_key_name()
            {
                // Arrange
                var schema = this.config.KeyExplicit();

                // Act
                var sql = this.config.Dialect.MakeFindStatement(schema, 5);

                // Assert
                var expected = new SqlString(@"
SELECT [Key], [Name]
FROM [KeyExplicit]
WHERE [Key] = {0}", 5, null);

                Assert.Equal(expected, sql, SqlStringComparer.Instance);
            }

            [Fact]
            public void Uses_each_key_in_composite_key()
            {
                // Arrange
                var schema = this.config.CompositeKeys();

                // Act
                var sql = this.config.Dialect.MakeFindStatement(schema, new { key1 = 2, key2 = 3 });

                // Assert
                var expected = new SqlString(@"
SELECT [Key1], [Key2], [Name]
FROM [CompositeKeys]
WHERE [Key1] = {0} AND [Key2] = {1}", 2, 3, null);

                Assert.Equal(expected, sql, SqlStringComparer.Instance);
            }

            [Fact]
            public void Adds_alias_when_primary_key_is_aliased()
            {
                // Arrange
                var schema = this.config.KeyAlias();

                // Act
                var sql = this.config.Dialect.MakeFindStatement(schema, 5);

                // Assert
                var expected = new SqlString(@"
SELECT [Key] AS [Id], [Name]
FROM [KeyAlias]
WHERE [Key] = {0}", 5, null);

                Assert.Equal(expected, sql, SqlStringComparer.Instance);
            }

            [Fact]
            public void Adds_alias_when_column_name_is_aliased()
            {
                // Arrange
                var schema = this.config.PropertyAlias();

                // Act
                var sql = this.config.Dialect.MakeFindStatement(schema, 5);

                // Assert
                var expected = new SqlString(@"
SELECT [Id], [YearsOld] AS [Age]
FROM [PropertyAlias]
WHERE [Id] = {0}", 5, null);

                Assert.Equal(expected, sql, SqlStringComparer.Instance);
            }
        }

        public class MakeGetRangeStatement
            : SqlServer2012DialectTests
        {
            [Fact]
            public void Selects_from_given_table()
            {
                // Arrange
                var schema = this.config.User();

                // Act
                var sql = this.config.Dialect.MakeGetRangeStatement(schema, null);

                // Assert
                FormattableString expected = $@"
SELECT [Id], [Name], [Age]
FROM [Users]";

                Assert.Equal(expected, sql, SqlStringComparer.Instance);
            }

            [Fact]
            public void Adds_conditions_clause()
            {
                // Arrange
                var schema = this.config.User();

                // Act
                var sql = this.config.Dialect.MakeGetRangeStatement(schema, $"WHERE Age > {10}");

                // Assert
                var expected = new SqlString(@"
SELECT [Id], [Name], [Age]
FROM [Users]
WHERE Age > {0}", 10);

                Assert.Equal(expected, sql, SqlStringComparer.Instance);
            }

            [Fact]
            public void Uses_explicit_primary_key_name()
            {
                // Arrange
                var schema = this.config.KeyExplicit();

                // Act
                var sql = this.config.Dialect.MakeGetRangeStatement(schema, null);

                // Assert
                FormattableString expected = $@"
SELECT [Key], [Name]
FROM [KeyExplicit]";

                Assert.Equal(expected, sql, SqlStringComparer.Instance);
            }

            [Fact]
            public void Adds_alias_when_primary_key_is_aliased()
            {
                // Arrange
                var schema = this.config.KeyAlias();

                // Act
                var sql = this.config.Dialect.MakeGetRangeStatement(schema, null);

                // Assert
                FormattableString expected = $@"
SELECT [Key] AS [Id], [Name]
FROM [KeyAlias]";

                Assert.Equal(expected, sql, SqlStringComparer.Instance);
            }

            [Fact]
            public void Adds_alias_when_column_name_is_aliased()
            {
                // Arrange
                var schema = this.config.PropertyAlias();

                // Act
                var sql = this.config.Dialect.MakeGetRangeStatement(schema, null);

                // Assert
                FormattableString expected = $@"
SELECT [Id], [YearsOld] AS [Age]
FROM [PropertyAlias]";

                Assert.Equal(expected, sql, SqlStringComparer.Instance);
            }
        }

        public class MakeGetTopNStatement
            : SqlServer2012DialectTests
        {
            [Fact]
            public void Selects_from_given_table()
            {
                // Arrange
                var schema = this.config.User();

                // Act
                var sql = this.config.Dialect.MakeGetTopNStatement(schema, 1, null, "Name");

                // Assert
                var expected = new SqlString(@"
SELECT TOP 1 [Id], [Name], [Age]
FROM [Users]
ORDER BY Name");

                Assert.Equal(expected, sql, SqlStringComparer.Instance);
            }

            [Fact]
            public void Adds_conditions_clause()
            {
                // Arrange
                var schema = this.config.User();

                // Act
                var sql = this.config.Dialect.MakeGetTopNStatement(schema, 1, $"WHERE Name LIKE {"Foo%"}", "Name");

                // Assert
                var expected = new SqlString(@"
SELECT TOP 1 [Id], [Name], [Age]
FROM [Users]
WHERE Name LIKE {0}
ORDER BY Name", "Foo%");

                Assert.Equal(expected, sql, SqlStringComparer.Instance);
            }

            [Fact]
            public void Adds_alias_when_column_name_is_aliased()
            {
                // Arrange
                var schema = this.config.PropertyAlias();

                // Act
                var sql = this.config.Dialect.MakeGetTopNStatement(schema, 1, null, "Name");

                // Assert
                FormattableString expected = $@"
SELECT TOP 1 [Id], [YearsOld] AS [Age]
FROM [PropertyAlias]
ORDER BY Name";

                Assert.Equal(expected, sql, SqlStringComparer.Instance);
            }

            [Theory]
            [InlineData(null)]
            [InlineData("")]
            [InlineData(" ")]
            public void Does_not_order_when_no_orderby_given(string orderBy)
            {
                // Arrange
                var schema = this.config.User();

                // Act
                var sql = this.config.Dialect.MakeGetTopNStatement(schema, 1, null, orderBy);

                // Assert
                FormattableString expected = $@"
SELECT TOP 1 [Id], [Name], [Age]
FROM [Users]";

                Assert.Equal(expected, sql, SqlStringComparer.Instance);
            }
        }

        public class MakeGetPageStatement
            : SqlServer2012DialectTests
        {
            [Theory]
            [InlineData(null)]
            [InlineData("")]
            [InlineData(" ")]
            public void Throws_exception_when_order_by_is_empty(string orderBy)
            {
                // Arrange
                var schema = this.config.User();

                // Act / Assert
                Assert.Throws<ArgumentException>(
                    () => this.config.Dialect.MakeGetPageStatement(schema, new Page(1, 10, true, 0, 9), null, orderBy));
            }

            [Fact]
            public void Selects_from_given_table()
            {
                // Arrange
                var schema = this.config.User();

                // Act
                var sql = this.config.Dialect.MakeGetPageStatement(schema, new Page(1, 10, true, 0, 9), null, "Name");

                // Assert
                FormattableString expected = $@"
SELECT [Id], [Name], [Age]
FROM [Users]
ORDER BY Name
OFFSET 0 ROWS FETCH NEXT 10 ROWS ONLY";

                Assert.Equal(expected, sql, SqlStringComparer.Instance);
            }

            [Fact]
            public void Adds_conditions_clause()
            {
                // Arrange
                var schema = this.config.User();

                // Act
                var sql = this.config.Dialect.MakeGetPageStatement(schema, new Page(1, 10, true, 0, 9), $"WHERE Name LIKE {"Foo%"}", "Name");

                // Assert
                var expected = new SqlString(@"
SELECT [Id], [Name], [Age]
FROM [Users]
WHERE Name LIKE {0}
ORDER BY Name
OFFSET 0 ROWS FETCH NEXT 10 ROWS ONLY", "Foo%");

                Assert.Equal(expected, sql, SqlStringComparer.Instance);
            }

            [Fact]
            public void Adds_alias_when_column_name_is_aliased()
            {
                // Arrange
                var schema = this.config.PropertyAlias();

                // Act
                var sql = this.config.Dialect.MakeGetPageStatement(schema, new Page(1, 10, true, 0, 9), null, "Name");

                // Assert
                FormattableString expected = $@"
SELECT [Id], [YearsOld] AS [Age]
FROM [PropertyAlias]
ORDER BY Name
OFFSET 0 ROWS FETCH NEXT 10 ROWS ONLY";

                Assert.Equal(expected, sql, SqlStringComparer.Instance);
            }

            [Fact]
            public void Selects_second_page()
            {
                // Arrange
                var schema = this.config.User();

                // Act
                var sql = this.config.Dialect.MakeGetPageStatement(schema, new Page(2, 10, true, 10, 19), null, "Name");

                // Assert
                FormattableString expected = $@"
SELECT [Id], [Name], [Age]
FROM [Users]
ORDER BY Name
OFFSET 10 ROWS FETCH NEXT 10 ROWS ONLY";

                Assert.Equal(expected, sql, SqlStringComparer.Instance);
            }

            [Fact]
            public void Selects_appropriate_number_of_rows()
            {
                // Arrange
                var schema = this.config.User();

                // Act
                var sql = this.config.Dialect.MakeGetPageStatement(schema, new Page(2, 5, true, 5, 9), null, "Name");

                // Assert
                FormattableString expected = $@"
SELECT [Id], [Name], [Age]
FROM [Users]
ORDER BY Name
OFFSET 5 ROWS FETCH NEXT 5 ROWS ONLY";

                Assert.Equal(expected, sql, SqlStringComparer.Instance);
            }
        }

        public class MakeInsertStatement
            : SqlServer2012DialectTests
        {
            [Fact]
            public void Inserts_into_given_table()
            {
                // Arrange
                var schema = this.config.User();

                // Act
                var sql = this.config.Dialect.MakeInsertStatement(schema, new User { Name = "Foo", Age = 10 });

                // Assert
                var expected = new SqlString(@"
INSERT INTO [Users] ([Name], [Age])
VALUES ({1}, {2});", 0, "Foo", 10);

                Assert.Equal(expected, sql, SqlStringComparer.Instance);
            }

            [Fact]
            public void Adds_primary_key_if_its_not_generated_by_database()
            {
                // Arrange
                var schema = this.config.KeyNotGenerated();

                // Act
                var sql = this.config.Dialect.MakeInsertStatement(schema, new KeyNotGenerated { Id = 6, Name = "Foo" });

                // Assert
                var expected = new SqlString(@"
INSERT INTO [KeyNotGenerated] ([Id], [Name])
VALUES ({0}, {1});", 6, "Foo");

                Assert.Equal(expected, sql, SqlStringComparer.Instance);
            }

            [Fact]
            public void Does_not_include_computed_columns()
            {
                // Arrange
                var schema = this.config.PropertyComputed();

                // Act
                var sql = this.config.Dialect.MakeInsertStatement(schema, new PropertyComputed { Name = "Foo" });

                // Assert
                var expected = new SqlString(@"
INSERT INTO [PropertyComputed] ([Name])
VALUES ({1});", 0, "Foo", default(DateTime));

                Assert.Equal(expected, sql, SqlStringComparer.Instance);
            }

            [Fact]
            public void Does_not_include_generated_columns()
            {
                // Arrange
                var schema = this.config.PropertyGenerated();

                // Act
                var sql = this.config.Dialect.MakeInsertStatement(schema, new PropertyGenerated { Name = "Foo" });

                // Assert
                var expected = new SqlString(@"
INSERT INTO [PropertyGenerated] ([Name])
VALUES ({1});", 0, "Foo", default(DateTime));

                Assert.Equal(expected, sql, SqlStringComparer.Instance);
            }
        }

        public class MakeInsertReturningIdentityStatement
            : SqlServer2012DialectTests
        {
            [Fact]
            public void Inserts_into_given_table()
            {
                // Arrange
                var schema = this.config.User();

                // Act
                var sql = this.config.Dialect.MakeInsertReturningIdentityStatement(schema, new User { Name = "Foo", Age = 10 });

                // Assert
                var expected = new SqlString(@"
INSERT INTO [Users] ([Name], [Age])
VALUES ({1}, {2});
SELECT CAST(SCOPE_IDENTITY() AS BIGINT) AS [id]", 0, "Foo", 10);

                Assert.Equal(expected, sql, SqlStringComparer.Instance);
            }

            [Fact]
            public void Adds_primary_key_if_its_not_generated_by_database()
            {
                // Arrange
                var schema = this.config.KeyNotGenerated();

                // Act
                var sql = this.config.Dialect.MakeInsertReturningIdentityStatement(schema, new KeyNotGenerated { Id = 10, Name = "Foo" });

                // Assert
                var expected = new SqlString(@"
INSERT INTO [KeyNotGenerated] ([Id], [Name])
VALUES ({0}, {1});
SELECT CAST(SCOPE_IDENTITY() AS BIGINT) AS [id]", 10, "Foo");

                Assert.Equal(expected, sql, SqlStringComparer.Instance);
            }

            [Fact]
            public void Does_not_include_computed_columns()
            {
                // Arrange
                var schema = this.config.PropertyComputed();

                // Act
                var sql = this.config.Dialect.MakeInsertReturningIdentityStatement(schema, new PropertyComputed { Name = "Foo" });

                // Assert
                var expected = new SqlString(@"
INSERT INTO [PropertyComputed] ([Name])
VALUES ({1});
SELECT CAST(SCOPE_IDENTITY() AS BIGINT) AS [id]", 0, "Foo", default(DateTime));

                Assert.Equal(expected, sql, SqlStringComparer.Instance);
            }

            [Fact]
            public void Does_not_include_generated_columns()
            {
                // Arrange
                var schema = this.config.PropertyGenerated();

                // Act
                var sql = this.config.Dialect.MakeInsertReturningIdentityStatement(schema, new PropertyGenerated { Name = "Foo" });

                // Assert
                var expected = new SqlString(@"
INSERT INTO [PropertyGenerated] ([Name])
VALUES ({1});
SELECT CAST(SCOPE_IDENTITY() AS BIGINT) AS [id]", 0, "Foo", default(DateTime));

                Assert.Equal(expected, sql, SqlStringComparer.Instance);
            }
        }

        public class MakeUpdateStatement
            : SqlServer2012DialectTests
        {
            [Fact]
            public void Updates_given_table()
            {
                // Arrange
                var schema = this.config.User();

                // Act
                var sql = this.config.Dialect.MakeUpdateStatement(schema, new User { Id = 5, Name = "Foo", Age = 10 });

                // Assert
                var expected = new SqlString(@"
UPDATE [Users]
SET [Name] = {1}, [Age] = {2}
WHERE [Id] = {0}", 5, "Foo", 10);

                Assert.Equal(expected, sql, SqlStringComparer.Instance);
            }

            [Fact]
            public void Uses_each_key_in_composite_key()
            {
                // Arrange
                var schema = this.config.CompositeKeys();

                // Act
                var sql = this.config.Dialect.MakeUpdateStatement(schema, new CompositeKeys { Key1 = 7, Key2 = 8, Name = "Foo" });

                // Assert
                var expected = new SqlString(@"
UPDATE [CompositeKeys]
SET [Name] = {2}
WHERE [Key1] = {0} AND [Key2] = {1}", 7, 8, "Foo");

                Assert.Equal(expected, sql, SqlStringComparer.Instance);
            }

            [Fact]
            public void Does_not_update_primary_key_even_if_its_not_auto_generated()
            {
                // Arrange
                var schema = this.config.KeyNotGenerated();

                // Act
                var sql = this.config.Dialect.MakeUpdateStatement(schema, new KeyNotGenerated { Id = 7, Name = "Foo" });

                // Assert
                var expected = new SqlString(@"
UPDATE [KeyNotGenerated]
SET [Name] = {1}
WHERE [Id] = {0}", 7, "Foo");

                Assert.Equal(expected, sql, SqlStringComparer.Instance);
            }

            [Fact]
            public void Uses_aliased_property_names()
            {
                // Arrange
                var schema = this.config.PropertyAlias();

                // Act
                var sql = this.config.Dialect.MakeUpdateStatement(schema, new PropertyAlias { Id = 5, Age = 10 });

                // Assert
                var expected = new SqlString(@"
UPDATE [PropertyAlias]
SET [YearsOld] = {1}
WHERE [Id] = {0}", 5, 10);

                Assert.Equal(expected, sql, SqlStringComparer.Instance);
            }

            [Fact]
            public void Uses_aliased_key_name()
            {
                // Arrange
                var schema = this.config.KeyAlias();

                // Act
                var sql = this.config.Dialect.MakeUpdateStatement(schema, new { Name = "Foo", Id = 10 });

                // Assert
                var expected = new SqlString(@"
UPDATE [KeyAlias]
SET [Name] = {1}
WHERE [Key] = {0}", 10, "Foo");

                Assert.Equal(expected, sql, SqlStringComparer.Instance);
            }

            [Fact]
            public void Uses_explicit_key_name()
            {
                // Arrange
                var schema = this.config.KeyExplicit();

                // Act
                var sql = this.config.Dialect.MakeUpdateStatement(schema, new KeyExplicit { Name = "Foo", Key = 10 });

                // Assert
                var expected = new SqlString(@"
UPDATE [KeyExplicit]
SET [Name] = {1}
WHERE [Key] = {0}", 10, "Foo");

                Assert.Equal(expected, sql, SqlStringComparer.Instance);
            }

            [Fact]
            public void Does_not_include_computed_columns()
            {
                // Arrange
                var schema = this.config.PropertyComputed();

                // Act
                var sql = this.config.Dialect.MakeUpdateStatement(schema, new PropertyComputed { Name = "Foo", Id = 10 });

                // Assert
                var expected = new SqlString(@"
UPDATE [PropertyComputed]
SET [Name] = {1}
WHERE [Id] = {0}", 10, "Foo", default(DateTime));

                Assert.Equal(expected, sql, SqlStringComparer.Instance);
            }

            [Fact]
            public void Includes_generated_columns()
            {
                // Arrange
                var schema = this.config.PropertyGenerated();

                // Act
                var sql = this.config.Dialect.MakeUpdateStatement(schema, new PropertyGenerated { Id = 5, Name = "Foo", Created = new DateTime(2018, 4, 1) });

                // Assert
                var expected = new SqlString(@"
UPDATE [PropertyGenerated]
SET [Name] = {1}, [Created] = {2}
WHERE [Id] = {0}", 5, "Foo", new DateTime(2018, 4, 1));

                Assert.Equal(expected, sql, SqlStringComparer.Instance);
            }
        }

        public class MakeDeleteByPrimaryKeyStatement
            : SqlServer2012DialectTests
        {
            [Fact]
            public void Deletes_from_given_table()
            {
                // Arrange
                var schema = this.config.User();

                // Act
                var sql = this.config.Dialect.MakeDeleteByPrimaryKeyStatement(schema, 5);

                // Assert
                var expected = new SqlString(@"
DELETE FROM [Users]
WHERE [Id] = {0}", 5, null, null);

                Assert.Equal(expected, sql, SqlStringComparer.Instance);
            }

            [Fact]
            public void Uses_each_key_in_composite_key()
            {
                // Arrange
                var schema = this.config.CompositeKeys();

                // Act
                var sql = this.config.Dialect.MakeDeleteByPrimaryKeyStatement(schema, new CompositeKeys { Key1 = 1, Key2 = 2 });

                // Assert
                var expected = new SqlString(@"
DELETE FROM [CompositeKeys]
WHERE [Key1] = {0} AND [Key2] = {1}", 1, 2, null as string);

                Assert.Equal(expected, sql, SqlStringComparer.Instance);
            }

            [Fact]
            public void Uses_primary_key_even_if_its_not_auto_generated()
            {
                // Arrange
                var schema = this.config.KeyNotGenerated();

                // Act
                var sql = this.config.Dialect.MakeDeleteByPrimaryKeyStatement(schema, 5);

                // Assert
                var expected = new SqlString(@"
DELETE FROM [KeyNotGenerated]
WHERE [Id] = {0}", 5, null);

                Assert.Equal(expected, sql, SqlStringComparer.Instance);
            }

            [Fact]
            public void Uses_aliased_key_name()
            {
                // Arrange
                var schema = this.config.KeyAlias();

                // Act
                var sql = this.config.Dialect.MakeDeleteByPrimaryKeyStatement(schema, 5);

                // Assert
                var expected = new SqlString(@"
DELETE FROM [KeyAlias]
WHERE [Key] = {0}", 5, null);

                Assert.Equal(expected, sql, SqlStringComparer.Instance);
            }

            [Fact]
            public void Uses_explicit_key_name()
            {
                // Arrange
                var schema = this.config.KeyExplicit();

                // Act
                var sql = this.config.Dialect.MakeDeleteByPrimaryKeyStatement(schema, 5);

                // Assert
                var expected = new SqlString(@"
DELETE FROM [KeyExplicit]
WHERE [Key] = {0}", 5, null);

                Assert.Equal(expected, sql, SqlStringComparer.Instance);
            }
        }

        public class MakeDeleteRangeStatement
            : SqlServer2012DialectTests
        {
            [Fact]
            public void Deletes_from_given_table()
            {
                // Arrange
                var schema = this.config.User();

                // Act
                var sql = this.config.Dialect.MakeDeleteRangeStatement(schema, $"WHERE [Age] > {10}");

                // Assert
                var expected = new SqlString(@"
DELETE FROM [Users]
WHERE [Age] > {0}", 10);

                Assert.Equal(expected, sql, SqlStringComparer.Instance);
            }
        }

        public class MakeWhereClause
            : SqlServer2012DialectTests
        {
            [Fact]
            public void Returns_empty_string_for_empty_conditions_object()
            {
                // Arrange
                var conditions = new { };
                var schema = this.GetConditionsSchema<User>(conditions);

                // Act
                var sql = this.config.Dialect.MakeWhereClause(schema, conditions);

                // Assert
                FormattableString expected = $"";
                Assert.Equal(expected, sql, SqlStringComparer.Instance);
            }

            [Fact]
            public void Includes_column_in_where_clause()
            {
                // Arrange
                var conditions = new { Name = "Bobby" };
                var schema = this.GetConditionsSchema<User>(conditions);

                // Act
                var sql = this.config.Dialect.MakeWhereClause(schema, conditions);

                // Assert
                var expected = new SqlString("WHERE [Name] = {1}", null, "Bobby");

                Assert.Equal(expected, sql, SqlStringComparer.Instance);
            }

            [Fact]
            public void Includes_all_columns_in_where_clause()
            {
                // Arrange
                var conditions = new { Name = "Bobby", Age = 5 };
                var schema = this.GetConditionsSchema<User>(conditions);

                // Act
                var sql = this.config.Dialect.MakeWhereClause(schema, conditions);

                // Assert
                FormattableString expected = new SqlString(@"WHERE [Name] = {1} AND [Age] = {2}", null, "Bobby", 5);

                Assert.Equal(expected, sql, SqlStringComparer.Instance);
            }

            [Fact]
            public void Checks_for_null_when_condition_value_is_null()
            {
                // Arrange
                var conditions = new { Name = (string)null };
                var schema = this.GetConditionsSchema<User>(conditions);

                // Act
                var sql = this.config.Dialect.MakeWhereClause(schema, conditions);

                // Assert
                var expected = new SqlString("WHERE [Name] IS NULL", null, null);

                Assert.Equal(expected, sql, SqlStringComparer.Instance);
            }

            private ImmutableArray<ConditionColumnSchema> GetConditionsSchema<TEntity>(object value)
            {
                var tableSchema = this.config.GetTableSchema(typeof(TEntity));
                return this.config.GetConditionsSchema(typeof(TEntity), tableSchema, value.GetType());
            }
        }

        public class MakeCreateTempTableStatement
            : SqlServer2012DialectTests
        {
            private readonly Mock<ITableNameFactory> tableNameFactory;

            public MakeCreateTempTableStatement()
            {
                this.tableNameFactory = new Mock<ITableNameFactory>();

                var defaultTableNameFactory = new AtttributeTableNameFactory();
                this.tableNameFactory.Setup(f => f.GetTableName(It.IsAny<Type>(), It.IsAny<IDialect>()))
                    .Returns((Type type, IDialect d) => "[#" + defaultTableNameFactory.GetTableName(type, d).Substring(1));

                this.config = this.config.AddSqlTypeMapping(typeof(DateTime), DbType.DateTime2).WithTableNameFactory(this.tableNameFactory.Object);
            }

            [Fact]
            public void Throws_exception_when_tablename_doesnt_begin_with_a_hash()
            {
                // Arrange
                this.tableNameFactory.Setup(f => f.GetTableName(It.IsAny<Type>(), It.IsAny<IDialect>()))
                    .Returns((Type type, IDialect d) => "table");

                // Act
                Assert.Throws<ArgumentException>(() => this.config.Dialect.MakeCreateTempTableStatement(this.config.User()));
            }

            [Fact]
            public void Throws_exception_if_there_are_no_columns()
            {
                // Act
                Assert.Throws<ArgumentException>(() => this.config.Dialect.MakeCreateTempTableStatement(this.config.NoColumns()));
            }

            [Fact]
            public void Creates_table_with_all_possible_types()
            {
                // Act
                var sql = this.config.Dialect.MakeCreateTempTableStatement(this.config.TempAllPossibleTypes());

                // Assert
                FormattableString expected = $@"
CREATE TABLE [#TempAllPossibleTypes]
(
    [Id] INT NOT NULL,
    [Int16Property] SMALLINT NOT NULL,
    [NullableInt16Property] SMALLINT NULL,
    [Int32Property] INT NOT NULL,
    [NullableInt32Property] INT NULL,
    [Int64Property] BIGINT NOT NULL,
    [NullableInt64Property] BIGINT NULL,
    [SingleProperty] REAL NOT NULL,
    [NullableSingleProperty] REAL NULL,
    [DoubleProperty] FLOAT NOT NULL,
    [NullableDoubleProperty] FLOAT NULL,
    [DecimalProperty] NUMERIC NOT NULL,
    [NullableDecimalProperty] NUMERIC NULL,
    [BoolProperty] BIT NOT NULL,
    [NullableBoolProperty] BIT NULL,
    [StringProperty] NVARCHAR(MAX) NOT NULL,
    [NullableStringProperty] NVARCHAR(MAX) NULL,
    [FixedLengthStringProperty] NVARCHAR(50) NULL,
    [CharProperty] NCHAR(1) NOT NULL,
    [NullableCharProperty] NCHAR(1) NULL,
    [GuidProperty] UNIQUEIDENTIFIER NOT NULL,
    [NullableGuidProperty] UNIQUEIDENTIFIER NULL,
    [DateTimeProperty] DATETIME2(7) NOT NULL,
    [NullableDateTimeProperty] DATETIME2(7) NULL,
    [DateTimeOffsetProperty] DATETIMEOFFSET NOT NULL,
    [NullableDateTimeOffsetProperty] DATETIMEOFFSET NULL,
    [ByteArrayProperty] VARBINARY(MAX) NOT NULL,
    [Color] INT NOT NULL,
    [NullableColor] INT NULL
);";

                Assert.Equal(expected, sql, SqlStringComparer.Instance);
            }
        }
    }
}