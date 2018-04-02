namespace PeregrineDb.Tests.Dialects
{
    using System;
    using System.Collections.Generic;
    using System.Collections.Immutable;
    using System.Data;
    using System.Diagnostics.CodeAnalysis;
    using Moq;
    using Pagination;
    using PeregrineDb;
    using PeregrineDb.Dialects.SqlServer2012;
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
                var schema = this.config.Dog();

                // Act
                var sql = this.config.Dialect.MakeCountCommand(schema, null);

                // Assert
                var expected = new SqlCommand(@"
SELECT COUNT(*)
FROM [Dogs]");

                Assert.Equal(expected, sql, SqlCommandComparer.Instance);
            }

            [Fact]
            public void Adds_conditions()
            {
                // Arrange
                var schema = this.config.Dog();

                // Act
                var sql = this.config.Dialect.MakeCountCommand(schema, $"WHERE Foo IS NOT NULL");

                // Assert
                var expected = new SqlCommand(@"
SELECT COUNT(*)
FROM [Dogs]
WHERE Foo IS NOT NULL");

                Assert.Equal(expected, sql, SqlCommandComparer.Instance);
            }
        }

        public class MakeFindStatement
            : SqlServer2012DialectTests
        {
            [Fact]
            public void Selects_from_given_table()
            {
                // Arrange
                var schema = this.config.Dog();

                // Act
                var sql = this.config.Dialect.MakeFindCommand(schema, 5);

                // Assert
                var expected = new SqlCommand(@"
SELECT [Id], [Name], [Age]
FROM [Dogs]
WHERE [Id] = @Id",
                    new Dictionary<string, object>
                        {
                            ["Id"] = 5
                        });

                Assert.Equal(expected, sql, SqlCommandComparer.Instance);
            }

            [Fact]
            public void Uses_non_default_primary_key_name()
            {
                // Arrange
                var schema = this.config.KeyExplicit();

                // Act
                var sql = this.config.Dialect.MakeFindCommand(schema, 5);

                // Assert
                var expected = new SqlCommand(@"
SELECT [Key], [Name]
FROM [KeyExplicit]
WHERE [Key] = @Key",
                    new Dictionary<string, object>
                        {
                            ["Key"] = 5
                        });

                Assert.Equal(expected, sql, SqlCommandComparer.Instance);
            }

            [Fact]
            public void Uses_each_key_in_composite_key()
            {
                // Arrange
                var schema = this.config.CompositeKeys();

                // Act
                var sql = this.config.Dialect.MakeFindCommand(schema, new { key1 = 2, key2 = 3 });

                // Assert
                var expected = new SqlCommand(@"
SELECT [Key1], [Key2], [Name]
FROM [CompositeKeys]
WHERE [Key1] = @Key1 AND [Key2] = @Key2",
                    new Dictionary<string, object>
                        {
                            ["key1"] = 2,
                            ["key2"] = 3
                        });

                Assert.Equal(expected, sql, SqlCommandComparer.Instance);
            }

            [Fact]
            public void Adds_alias_when_primary_key_is_aliased()
            {
                // Arrange
                var schema = this.config.KeyAlias();

                // Act
                var sql = this.config.Dialect.MakeFindCommand(schema, 5);

                // Assert
                var expected = new SqlCommand(@"
SELECT [Key] AS [Id], [Name]
FROM [KeyAlias]
WHERE [Key] = @Id",
                    new Dictionary<string, object>
                        {
                            ["Id"] = 5
                        });

                Assert.Equal(expected, sql, SqlCommandComparer.Instance);
            }

            [Fact]
            public void Adds_alias_when_column_name_is_aliased()
            {
                // Arrange
                var schema = this.config.PropertyAlias();

                // Act
                var sql = this.config.Dialect.MakeFindCommand(schema, 5);

                // Assert
                var expected = new SqlCommand(@"
SELECT [Id], [YearsOld] AS [Age]
FROM [PropertyAlias]
WHERE [Id] = @Id",
                    new Dictionary<string, object>
                        {
                            ["Id"] = 5
                        });

                Assert.Equal(expected, sql, SqlCommandComparer.Instance);
            }
        }

        public class MakeGetRangeStatement
            : SqlServer2012DialectTests
        {
            [Fact]
            public void Selects_from_given_table()
            {
                // Arrange
                var schema = this.config.Dog();

                // Act
                var sql = this.config.Dialect.MakeGetRangeCommand(schema, null);

                // Assert
                var expected = new SqlCommand(@"
SELECT [Id], [Name], [Age]
FROM [Dogs]");

                Assert.Equal(expected, sql, SqlCommandComparer.Instance);
            }

            [Fact]
            public void Adds_conditions_clause()
            {
                // Arrange
                var schema = this.config.Dog();

                // Act
                var sql = this.config.Dialect.MakeGetRangeCommand(schema, $"WHERE Age > {10}");

                // Assert
                var expected = new SqlCommand(@"
SELECT [Id], [Name], [Age]
FROM [Dogs]
WHERE Age > @p0",
                    new Dictionary<string, object>
                        {
                            ["p0"] = 10
                        });

                Assert.Equal(expected, sql, SqlCommandComparer.Instance);
            }

            [Fact]
            public void Uses_explicit_primary_key_name()
            {
                // Arrange
                var schema = this.config.KeyExplicit();

                // Act
                var sql = this.config.Dialect.MakeGetRangeCommand(schema, null);

                // Assert
                var expected = new SqlCommand(@"
SELECT [Key], [Name]
FROM [KeyExplicit]");

                Assert.Equal(expected, sql, SqlCommandComparer.Instance);
            }

            [Fact]
            public void Adds_alias_when_primary_key_is_aliased()
            {
                // Arrange
                var schema = this.config.KeyAlias();

                // Act
                var sql = this.config.Dialect.MakeGetRangeCommand(schema, null);

                // Assert
                var expected = new SqlCommand(@"
SELECT [Key] AS [Id], [Name]
FROM [KeyAlias]");

                Assert.Equal(expected, sql, SqlCommandComparer.Instance);
            }

            [Fact]
            public void Adds_alias_when_column_name_is_aliased()
            {
                // Arrange
                var schema = this.config.PropertyAlias();

                // Act
                var sql = this.config.Dialect.MakeGetRangeCommand(schema, null);

                // Assert
                var expected = new SqlCommand(@"
SELECT [Id], [YearsOld] AS [Age]
FROM [PropertyAlias]");

                Assert.Equal(expected, sql, SqlCommandComparer.Instance);
            }
        }

        public class MakeGetTopNStatement
            : SqlServer2012DialectTests
        {
            [Fact]
            public void Selects_from_given_table()
            {
                // Arrange
                var schema = this.config.Dog();

                // Act
                var sql = this.config.Dialect.MakeGetTopNCommand(schema, 1, null, "Name");

                // Assert
                var expected = new SqlCommand(@"
SELECT TOP 1 [Id], [Name], [Age]
FROM [Dogs]
ORDER BY Name");

                Assert.Equal(expected, sql, SqlCommandComparer.Instance);
            }

            [Fact]
            public void Adds_conditions_clause()
            {
                // Arrange
                var schema = this.config.Dog();

                // Act
                var sql = this.config.Dialect.MakeGetTopNCommand(schema, 1, $"WHERE Name LIKE {"Foo%"}", "Name");

                // Assert
                var expected = new SqlCommand(@"
SELECT TOP 1 [Id], [Name], [Age]
FROM [Dogs]
WHERE Name LIKE @p0
ORDER BY Name",
                    new Dictionary<string, object>
                        {
                            ["p0"] = "Foo%"
                        });

                Assert.Equal(expected, sql, SqlCommandComparer.Instance);
            }

            [Fact]
            public void Adds_alias_when_column_name_is_aliased()
            {
                // Arrange
                var schema = this.config.PropertyAlias();

                // Act
                var sql = this.config.Dialect.MakeGetTopNCommand(schema, 1, null, "Name");

                // Assert
                var expected = new SqlCommand(@"
SELECT TOP 1 [Id], [YearsOld] AS [Age]
FROM [PropertyAlias]
ORDER BY Name");

                Assert.Equal(expected, sql, SqlCommandComparer.Instance);
            }

            [Theory]
            [InlineData(null)]
            [InlineData("")]
            [InlineData(" ")]
            public void Does_not_order_when_no_orderby_given(string orderBy)
            {
                // Arrange
                var schema = this.config.Dog();

                // Act
                var sql = this.config.Dialect.MakeGetTopNCommand(schema, 1, null, orderBy);

                // Assert
                var expected = new SqlCommand(@"
SELECT TOP 1 [Id], [Name], [Age]
FROM [Dogs]");

                Assert.Equal(expected, sql, SqlCommandComparer.Instance);
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
                var schema = this.config.Dog();

                // Act / Assert
                Assert.Throws<ArgumentException>(
                    () => this.config.Dialect.MakeGetPageCommand(schema, new Page(1, 10, true, 0, 9), null, orderBy));
            }

            [Fact]
            public void Selects_from_given_table()
            {
                // Arrange
                var schema = this.config.Dog();

                // Act
                var sql = this.config.Dialect.MakeGetPageCommand(schema, new Page(1, 10, true, 0, 9), null, "Name");

                // Assert
                var expected = new SqlCommand(@"
SELECT [Id], [Name], [Age]
FROM [Dogs]
ORDER BY Name
OFFSET 0 ROWS FETCH NEXT 10 ROWS ONLY");

                Assert.Equal(expected, sql, SqlCommandComparer.Instance);
            }

            [Fact]
            public void Adds_conditions_clause()
            {
                // Arrange
                var schema = this.config.Dog();

                // Act
                var sql = this.config.Dialect.MakeGetPageCommand(schema, new Page(1, 10, true, 0, 9), $"WHERE Name LIKE {"Foo%"}", "Name");

                // Assert
                var expected = new SqlCommand(@"
SELECT [Id], [Name], [Age]
FROM [Dogs]
WHERE Name LIKE @p0
ORDER BY Name
OFFSET 0 ROWS FETCH NEXT 10 ROWS ONLY",
                    new Dictionary<string, object>
                        {
                            ["p0"] = "Foo%"
                        });

                Assert.Equal(expected, sql, SqlCommandComparer.Instance);
            }

            [Fact]
            public void Adds_alias_when_column_name_is_aliased()
            {
                // Arrange
                var schema = this.config.PropertyAlias();

                // Act
                var sql = this.config.Dialect.MakeGetPageCommand(schema, new Page(1, 10, true, 0, 9), null, "Name");

                // Assert
                var expected = new SqlCommand(@"
SELECT [Id], [YearsOld] AS [Age]
FROM [PropertyAlias]
ORDER BY Name
OFFSET 0 ROWS FETCH NEXT 10 ROWS ONLY");

                Assert.Equal(expected, sql, SqlCommandComparer.Instance);
            }

            [Fact]
            public void Selects_second_page()
            {
                // Arrange
                var schema = this.config.Dog();

                // Act
                var sql = this.config.Dialect.MakeGetPageCommand(schema, new Page(2, 10, true, 10, 19), null, "Name");

                // Assert
                var expected = new SqlCommand(@"
SELECT [Id], [Name], [Age]
FROM [Dogs]
ORDER BY Name
OFFSET 10 ROWS FETCH NEXT 10 ROWS ONLY");

                Assert.Equal(expected, sql, SqlCommandComparer.Instance);
            }

            [Fact]
            public void Selects_appropriate_number_of_rows()
            {
                // Arrange
                var schema = this.config.Dog();

                // Act
                var sql = this.config.Dialect.MakeGetPageCommand(schema, new Page(2, 5, true, 5, 9), null, "Name");

                // Assert
                var expected = new SqlCommand(@"
SELECT [Id], [Name], [Age]
FROM [Dogs]
ORDER BY Name
OFFSET 5 ROWS FETCH NEXT 5 ROWS ONLY");

                Assert.Equal(expected, sql, SqlCommandComparer.Instance);
            }
        }

        public class MakeInsertStatement
            : SqlServer2012DialectTests
        {
            [Fact]
            public void Inserts_into_given_table()
            {
                // Arrange
                var schema = this.config.Dog();

                // Act
                var sql = this.config.Dialect.MakeInsertCommand(schema, new Dog { Name = "Foo", Age = 10 });

                // Assert
                var expected = new SqlCommand(@"
INSERT INTO [Dogs] ([Name], [Age])
VALUES (@Name, @Age);",
                    new Dictionary<string, object>
                        {
                            ["Id"] = 0,
                            ["Name"] = "Foo",
                            ["Age"] = 10
                        });

                Assert.Equal(expected, sql, SqlCommandComparer.Instance);
            }

            [Fact]
            public void Adds_primary_key_if_its_not_generated_by_database()
            {
                // Arrange
                var schema = this.config.KeyNotGenerated();

                // Act
                var sql = this.config.Dialect.MakeInsertCommand(schema, new KeyNotGenerated { Id = 6, Name = "Foo" });

                // Assert
                var expected = new SqlCommand(@"
INSERT INTO [KeyNotGenerated] ([Id], [Name])
VALUES (@Id, @Name);",
                    new Dictionary<string, object>
                        {
                            ["Id"] = 6,
                            ["Name"] = "Foo"
                        });

                Assert.Equal(expected, sql, SqlCommandComparer.Instance);
            }

            [Fact]
            public void Does_not_include_computed_columns()
            {
                // Arrange
                var schema = this.config.PropertyComputed();

                // Act
                var sql = this.config.Dialect.MakeInsertCommand(schema, new PropertyComputed { Name = "Foo" });

                // Assert
                var expected = new SqlCommand(@"
INSERT INTO [PropertyComputed] ([Name])
VALUES (@Name);",
                    new Dictionary<string, object>
                        {
                            ["Id"] = 0,
                            ["Name"] = "Foo",
                            ["LastUpdated"] = default(DateTime)
                        });

                Assert.Equal(expected, sql, SqlCommandComparer.Instance);
            }

            [Fact]
            public void Does_not_include_generated_columns()
            {
                // Arrange
                var schema = this.config.PropertyGenerated();

                // Act
                var sql = this.config.Dialect.MakeInsertCommand(schema, new PropertyGenerated { Name = "Foo" });

                // Assert
                var expected = new SqlCommand(@"
INSERT INTO [PropertyGenerated] ([Name])
VALUES (@Name);",
                    new Dictionary<string, object>
                        {
                            ["Id"] = 0,
                            ["Name"] = "Foo",
                            ["Created"] = default(DateTime)
                        });

                Assert.Equal(expected, sql, SqlCommandComparer.Instance);
            }
        }

        public class MakeInsertReturningIdentityStatement
            : SqlServer2012DialectTests
        {
            [Fact]
            public void Inserts_into_given_table()
            {
                // Arrange
                var schema = this.config.Dog();

                // Act
                var sql = this.config.Dialect.MakeInsertReturningIdentityCommand(schema, new Dog { Name = "Foo", Age = 10 });

                // Assert
                var expected = new SqlCommand(@"
INSERT INTO [Dogs] ([Name], [Age])
VALUES (@Name, @Age);
SELECT CAST(SCOPE_IDENTITY() AS BIGINT) AS [id]",
                    new Dictionary<string, object>
                        {
                            ["Id"] = 0,
                            ["Age"] = 10,
                            ["Name"] = "Foo"
                        });

                Assert.Equal(expected, sql, SqlCommandComparer.Instance);
            }

            [Fact]
            public void Adds_primary_key_if_its_not_generated_by_database()
            {
                // Arrange
                var schema = this.config.KeyNotGenerated();

                // Act
                var sql = this.config.Dialect.MakeInsertReturningIdentityCommand(schema, new KeyNotGenerated { Id = 10, Name = "Foo" });

                // Assert
                var expected = new SqlCommand(@"
INSERT INTO [KeyNotGenerated] ([Id], [Name])
VALUES (@Id, @Name);
SELECT CAST(SCOPE_IDENTITY() AS BIGINT) AS [id]",
                    new Dictionary<string, object>
                        {
                            ["Id"] = 10,
                            ["Name"] = "Foo"
                        });

                Assert.Equal(expected, sql, SqlCommandComparer.Instance);
            }

            [Fact]
            public void Does_not_include_computed_columns()
            {
                // Arrange
                var schema = this.config.PropertyComputed();

                // Act
                var sql = this.config.Dialect.MakeInsertReturningIdentityCommand(schema, new PropertyComputed { Name = "Foo" });

                // Assert
                var expected = new SqlCommand(@"
INSERT INTO [PropertyComputed] ([Name])
VALUES (@Name);
SELECT CAST(SCOPE_IDENTITY() AS BIGINT) AS [id]",
                    new Dictionary<string, object>
                        {
                            ["Id"] = 0,
                            ["Name"] = "Foo",
                            ["LastUpdated"] = default(DateTime)
                        });

                Assert.Equal(expected, sql, SqlCommandComparer.Instance);
            }

            [Fact]
            public void Does_not_include_generated_columns()
            {
                // Arrange
                var schema = this.config.PropertyGenerated();

                // Act
                var sql = this.config.Dialect.MakeInsertReturningIdentityCommand(schema, new PropertyGenerated { Name = "Foo" });

                // Assert
                var expected = new SqlCommand(@"
INSERT INTO [PropertyGenerated] ([Name])
VALUES (@Name);
SELECT CAST(SCOPE_IDENTITY() AS BIGINT) AS [id]",
                    new Dictionary<string, object>
                        {
                            ["Id"] = 0,
                            ["Name"] = "Foo",
                            ["Created"] = default(DateTime)
                        });

                Assert.Equal(expected, sql, SqlCommandComparer.Instance);
            }
        }

        public class MakeUpdateStatement
            : SqlServer2012DialectTests
        {
            [Fact]
            public void Updates_given_table()
            {
                // Arrange
                var schema = this.config.Dog();

                // Act
                var sql = this.config.Dialect.MakeUpdateCommand(schema, new Dog { Id = 5, Name = "Foo", Age = 10 });

                // Assert
                var expected = new SqlCommand(@"
UPDATE [Dogs]
SET [Name] = @Name, [Age] = @Age
WHERE [Id] = @Id",
                    new Dictionary<string, object>
                        {
                            ["Id"] = 5,
                            ["Name"] = "Foo",
                            ["Age"] = 10
                        });

                Assert.Equal(expected, sql, SqlCommandComparer.Instance);
            }

            [Fact]
            public void Uses_each_key_in_composite_key()
            {
                // Arrange
                var schema = this.config.CompositeKeys();

                // Act
                var sql = this.config.Dialect.MakeUpdateCommand(schema, new CompositeKeys { Key1 = 7, Key2 = 8, Name = "Foo" });

                // Assert
                var expected = new SqlCommand(@"
UPDATE [CompositeKeys]
SET [Name] = @Name
WHERE [Key1] = @Key1 AND [Key2] = @Key2",
                    new Dictionary<string, object>
                        {
                            ["Key1"] = 7,
                            ["Key2"] = 8,
                            ["Name"] = "Foo"
                        });

                Assert.Equal(expected, sql, SqlCommandComparer.Instance);
            }

            [Fact]
            public void Does_not_update_primary_key_even_if_its_not_auto_generated()
            {
                // Arrange
                var schema = this.config.KeyNotGenerated();

                // Act
                var sql = this.config.Dialect.MakeUpdateCommand(schema, new KeyNotGenerated { Id = 7, Name = "Foo" });

                // Assert
                var expected = new SqlCommand(@"
UPDATE [KeyNotGenerated]
SET [Name] = @Name
WHERE [Id] = @Id",
                    new Dictionary<string, object>
                        {
                            ["Id"] = 7,
                            ["Name"] = "Foo"
                        });

                Assert.Equal(expected, sql, SqlCommandComparer.Instance);
            }

            [Fact]
            public void Uses_aliased_property_names()
            {
                // Arrange
                var schema = this.config.PropertyAlias();

                // Act
                var sql = this.config.Dialect.MakeUpdateCommand(schema, new PropertyAlias { Id = 5, Age = 10 });

                // Assert
                var expected = new SqlCommand(@"
UPDATE [PropertyAlias]
SET [YearsOld] = @Age
WHERE [Id] = @Id",
                    new Dictionary<string, object>
                        {
                            ["Id"] = 5,
                            ["Age"] = 10
                        });

                Assert.Equal(expected, sql, SqlCommandComparer.Instance);
            }

            [Fact]
            public void Uses_aliased_key_name()
            {
                // Arrange
                var schema = this.config.KeyAlias();

                // Act
                var sql = this.config.Dialect.MakeUpdateCommand(schema, new KeyAlias { Name = "Foo", Id = 10 });

                // Assert
                var expected = new SqlCommand(@"
UPDATE [KeyAlias]
SET [Name] = @Name
WHERE [Key] = @Id",
                    new Dictionary<string, object>
                        {
                            ["Id"] = 10,
                            ["Name"] = "Foo"
                        });

                Assert.Equal(expected, sql, SqlCommandComparer.Instance);
            }

            [Fact]
            public void Uses_explicit_key_name()
            {
                // Arrange
                var schema = this.config.KeyExplicit();

                // Act
                var sql = this.config.Dialect.MakeUpdateCommand(schema, new KeyExplicit { Name = "Foo", Key = 10 });

                // Assert
                var expected = new SqlCommand(@"
UPDATE [KeyExplicit]
SET [Name] = @Name
WHERE [Key] = @Key",
                    new Dictionary<string, object>
                        {
                            ["Key"] = 10,
                            ["Name"] = "Foo"
                        });

                Assert.Equal(expected, sql, SqlCommandComparer.Instance);
            }

            [Fact]
            public void Does_not_include_computed_columns()
            {
                // Arrange
                var schema = this.config.PropertyComputed();

                // Act
                var sql = this.config.Dialect.MakeUpdateCommand(schema, new PropertyComputed { Name = "Foo", Id = 10 });

                // Assert
                var expected = new SqlCommand(@"
UPDATE [PropertyComputed]
SET [Name] = @Name
WHERE [Id] = @Id",
                    new Dictionary<string, object>
                        {
                            ["Id"] = 10,
                            ["Name"] = "Foo",
                            ["LastUpdated"] = default(DateTime)
                        });

                Assert.Equal(expected, sql, SqlCommandComparer.Instance);
            }

            [Fact]
            public void Includes_generated_columns()
            {
                // Arrange
                var schema = this.config.PropertyGenerated();

                // Act
                var sql = this.config.Dialect.MakeUpdateCommand(schema, new PropertyGenerated { Id = 5, Name = "Foo", Created = new DateTime(2018, 4, 1) });

                // Assert
                var expected = new SqlCommand(@"
UPDATE [PropertyGenerated]
SET [Name] = @Name, [Created] = @Created
WHERE [Id] = @Id",
                    new Dictionary<string, object>
                        {
                            ["Id"] = 5,
                            ["Name"] = "Foo",
                            ["Created"] = new DateTime(2018, 4, 1)
                        });

                Assert.Equal(expected, sql, SqlCommandComparer.Instance);
            }
        }

        public class MakeDeleteByPrimaryKeyStatement
            : SqlServer2012DialectTests
        {
            [Fact]
            public void Deletes_from_given_table()
            {
                // Arrange
                var schema = this.config.Dog();

                // Act
                var sql = this.config.Dialect.MakeDeleteByPrimaryKeyCommand(schema, 5);

                // Assert
                var expected = new SqlCommand(@"
DELETE FROM [Dogs]
WHERE [Id] = @Id",
                    new Dictionary<string, object>
                        {
                            ["Id"] = 5
                        });

                Assert.Equal(expected, sql, SqlCommandComparer.Instance);
            }

            [Fact]
            public void Uses_each_key_in_composite_key()
            {
                // Arrange
                var schema = this.config.CompositeKeys();

                // Act
                var sql = this.config.Dialect.MakeDeleteByPrimaryKeyCommand(schema, new CompositeKeys { Key1 = 1, Key2 = 2 });

                // Assert
                var expected = new SqlCommand(@"
DELETE FROM [CompositeKeys]
WHERE [Key1] = @Key1 AND [Key2] = @Key2",
                    new Dictionary<string, object>
                        {
                            ["Key1"] = 1,
                            ["Key2"] = 2,
                            ["Name"] = null
                        });

                Assert.Equal(expected, sql, SqlCommandComparer.Instance);
            }

            [Fact]
            public void Uses_primary_key_even_if_its_not_auto_generated()
            {
                // Arrange
                var schema = this.config.KeyNotGenerated();

                // Act
                var sql = this.config.Dialect.MakeDeleteByPrimaryKeyCommand(schema, 5);

                // Assert
                var expected = new SqlCommand(@"
DELETE FROM [KeyNotGenerated]
WHERE [Id] = @Id",
                    new Dictionary<string, object>
                        {
                            ["Id"] = 5
                        });

                Assert.Equal(expected, sql, SqlCommandComparer.Instance);
            }

            [Fact]
            public void Uses_aliased_key_name()
            {
                // Arrange
                var schema = this.config.KeyAlias();

                // Act
                var sql = this.config.Dialect.MakeDeleteByPrimaryKeyCommand(schema, 5);

                // Assert
                var expected = new SqlCommand(@"
DELETE FROM [KeyAlias]
WHERE [Key] = @Id",
                    new Dictionary<string, object>
                        {
                            ["Id"] = 5
                        });

                Assert.Equal(expected, sql, SqlCommandComparer.Instance);
            }

            [Fact]
            public void Uses_explicit_key_name()
            {
                // Arrange
                var schema = this.config.KeyExplicit();

                // Act
                var sql = this.config.Dialect.MakeDeleteByPrimaryKeyCommand(schema, 5);

                // Assert
                var expected = new SqlCommand(@"
DELETE FROM [KeyExplicit]
WHERE [Key] = @Key",
                    new Dictionary<string, object>
                        {
                            ["Key"] = 5
                        });

                Assert.Equal(expected, sql, SqlCommandComparer.Instance);
            }
        }

        public class MakeDeleteRangeStatement
            : SqlServer2012DialectTests
        {
            [Fact]
            public void Deletes_from_given_table()
            {
                // Arrange
                var schema = this.config.Dog();

                // Act
                var sql = this.config.Dialect.MakeDeleteRangeCommand(schema, $"WHERE [Age] > {10}");

                // Assert
                var expected = new SqlCommand(@"
DELETE FROM [Dogs]
WHERE [Age] > @p0",
                    new Dictionary<string, object>
                        {
                            ["p0"] = 10
                        });

                Assert.Equal(expected, sql, SqlCommandComparer.Instance);
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
                var schema = this.GetConditionsSchema<Dog>(conditions);

                // Act
                var sql = this.config.Dialect.MakeWhereClause(schema, conditions);

                // Assert
                var expected = new SqlString("");
                Assert.Equal(expected, sql, SqlStringComparer.Instance);
            }

            [Fact]
            public void Includes_column_in_where_clause()
            {
                // Arrange
                var conditions = new { Name = "Bobby" };
                var schema = this.GetConditionsSchema<Dog>(conditions);

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
                var schema = this.GetConditionsSchema<Dog>(conditions);

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
                var schema = this.GetConditionsSchema<Dog>(conditions);

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
            private readonly Mock<ITableNameConvention> tableNameFactory;

            public MakeCreateTempTableStatement()
            {
                this.tableNameFactory = new Mock<ITableNameConvention>();

                var defaultTableNameFactory = new AtttributeTableNameConvention(new SqlServer2012NameEscaper());
                this.tableNameFactory.Setup(f => f.GetTableName(It.IsAny<Type>()))
                    .Returns((Type type) => "[#" + defaultTableNameFactory.GetTableName(type).Substring(1));

                this.config = this.config.AddSqlTypeMapping(typeof(DateTime), DbType.DateTime2).WithTableNameConvention(this.tableNameFactory.Object);
            }

            [Fact]
            public void Throws_exception_when_tablename_doesnt_begin_with_a_hash()
            {
                // Arrange
                this.tableNameFactory.Setup(f => f.GetTableName(It.IsAny<Type>()))
                    .Returns((Type type) => "table");

                // Act
                Assert.Throws<ArgumentException>(() => this.config.Dialect.MakeCreateTempTableCommand(this.config.Dog()));
            }

            [Fact]
            public void Throws_exception_if_there_are_no_columns()
            {
                // Act
                Assert.Throws<ArgumentException>(() => this.config.Dialect.MakeCreateTempTableCommand(this.config.NoColumns()));
            }

            [Fact]
            public void Creates_table_with_all_possible_types()
            {
                // Act
                var sql = this.config.Dialect.MakeCreateTempTableCommand(this.config.TempAllPossibleTypes());

                // Assert
                var expected = new SqlCommand(@"
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
);");

                Assert.Equal(expected, sql, SqlCommandComparer.Instance);
            }
        }
    }
}