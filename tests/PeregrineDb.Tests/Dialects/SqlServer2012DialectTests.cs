namespace PeregrineDb.Tests.Dialects
{
    using System;
    using System.Collections.Generic;
    using System.Collections.Immutable;
    using System.Data;
    using System.Diagnostics.CodeAnalysis;
    using FluentAssertions;
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

        public class MakeCountStatementFromSql
            : SqlServer2012DialectTests
        {
            [Fact]
            public void Selects_from_given_table()
            {
                // Act
                var sql = this.config.Dialect.MakeCountCommand<Dog>(null);

                // Assert
                var expected = new SqlCommand(@"
SELECT COUNT(*)
FROM [Dogs]");

                Assert.Equal(expected, sql, SqlCommandComparer.Instance);
            }

            [Fact]
            public void Adds_conditions()
            {
                // Act
                var sql = this.config.Dialect.MakeCountCommand<Dog>($"WHERE Foo IS NOT NULL");

                // Assert
                var expected = new SqlCommand(@"
SELECT COUNT(*)
FROM [Dogs]
WHERE Foo IS NOT NULL");

                Assert.Equal(expected, sql, SqlCommandComparer.Instance);
            }
        }
        public class MakeCountStatementFromParameters
            : SqlServer2012DialectTests
        {
            [Fact]
            public void Errors_when_conditions_is_null()
            {
                // Act
                Action act = () => this.config.Dialect.MakeCountCommand<Dog>((object)null);

                // Assert
                act.ShouldThrow<ArgumentNullException>();
            }

            [Fact]
            public void Adds_conditions()
            {
                // Act
                var sql = this.config.Dialect.MakeCountCommand<Dog>(new { Name = (string)null });

                // Assert
                var expected = new SqlCommand(@"
SELECT COUNT(*)
FROM [Dogs]
WHERE [Name] IS NULL",
                    new Dictionary<string, object>
                        {
                            ["p0"] = null,
                            ["p1"] = null
                        });

                Assert.Equal(expected, sql, SqlCommandComparer.Instance);
            }
        }

        public class MakeFindStatement
            : SqlServer2012DialectTests
        {
            [Fact]
            public void Errors_when_id_is_null()
            {
                // Act
                Action act = () => this.config.Dialect.MakeFindCommand<Dog>(null);

                // Assert
                act.ShouldThrow<ArgumentNullException>();
            }

            [Fact]
            public void Selects_from_given_table()
            {
                // Act
                var sql = this.config.Dialect.MakeFindCommand<Dog>(5);

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
                // Act
                var sql = this.config.Dialect.MakeFindCommand<KeyExplicit>(5);

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
                // Act
                var sql = this.config.Dialect.MakeFindCommand<CompositeKeys>(new { key1 = 2, key2 = 3 });

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
                // Act
                var sql = this.config.Dialect.MakeFindCommand<KeyAlias>(5);

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
                // Act
                var sql = this.config.Dialect.MakeFindCommand<PropertyAlias>(5);

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

        public class MakeGetRangeStatementFromSql
            : SqlServer2012DialectTests
        {
            [Fact]
            public void Selects_from_given_table()
            {
                // Act
                var sql = this.config.Dialect.MakeGetRangeCommand<Dog>(null);

                // Assert
                var expected = new SqlCommand(@"
SELECT [Id], [Name], [Age]
FROM [Dogs]");

                Assert.Equal(expected, sql, SqlCommandComparer.Instance);
            }

            [Fact]
            public void Adds_conditions_clause()
            {
                // Act
                var sql = this.config.Dialect.MakeGetRangeCommand<Dog>($"WHERE Age > {10}");

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
                // Act
                var sql = this.config.Dialect.MakeGetRangeCommand<KeyExplicit>(null);

                // Assert
                var expected = new SqlCommand(@"
SELECT [Key], [Name]
FROM [KeyExplicit]");

                Assert.Equal(expected, sql, SqlCommandComparer.Instance);
            }

            [Fact]
            public void Adds_alias_when_primary_key_is_aliased()
            {
                // Act
                var sql = this.config.Dialect.MakeGetRangeCommand<KeyAlias>(null);

                // Assert
                var expected = new SqlCommand(@"
SELECT [Key] AS [Id], [Name]
FROM [KeyAlias]");

                Assert.Equal(expected, sql, SqlCommandComparer.Instance);
            }

            [Fact]
            public void Adds_alias_when_column_name_is_aliased()
            {
                // Act
                var sql = this.config.Dialect.MakeGetRangeCommand<PropertyAlias>(null);

                // Assert
                var expected = new SqlCommand(@"
SELECT [Id], [YearsOld] AS [Age]
FROM [PropertyAlias]");

                Assert.Equal(expected, sql, SqlCommandComparer.Instance);
            }
        }

        public class MakeGetRangeStatementFromParameters
            : SqlServer2012DialectTests
        {
            [Fact]
            public void Selects_from_given_table()
            {
                // Act
                var sql = this.config.Dialect.MakeGetRangeCommand<Dog>(new { Name = "Fido" });

                // Assert
                var expected = new SqlCommand(@"
SELECT [Id], [Name], [Age]
FROM [Dogs]
WHERE [Name] = @p1",
                    new Dictionary<string, object>
                        {
                            ["p0"] = null,
                            ["p1"] = "Fido"
                        });

                Assert.Equal(expected, sql, SqlCommandComparer.Instance);
            }

            [Fact]
            public void Uses_explicit_primary_key_name()
            {
                // Act
                var sql = this.config.Dialect.MakeGetRangeCommand<KeyExplicit>(new { Name = "Fido" });

                // Assert
                var expected = new SqlCommand(@"
SELECT [Key], [Name]
FROM [KeyExplicit]
WHERE [Name] = @p1",
                    new Dictionary<string, object>
                        {
                            ["p0"] = null,
                            ["p1"] = "Fido"
                        });

                Assert.Equal(expected, sql, SqlCommandComparer.Instance);
            }

            [Fact]
            public void Adds_alias_when_primary_key_is_aliased()
            {
                // Act
                var sql = this.config.Dialect.MakeGetRangeCommand<KeyAlias>(new { Name = "Fido" });

                // Assert
                var expected = new SqlCommand(@"
SELECT [Key] AS [Id], [Name]
FROM [KeyAlias]
WHERE [Name] = @p1",
                    new Dictionary<string, object>
                        {
                            ["p0"] = null,
                            ["p1"] = "Fido"
                        });

                Assert.Equal(expected, sql, SqlCommandComparer.Instance);
            }

            [Fact]
            public void Adds_alias_when_column_name_is_aliased()
            {
                // Act
                var sql = this.config.Dialect.MakeGetRangeCommand<PropertyAlias>(new { Age = 5 });

                // Assert
                var expected = new SqlCommand(@"
SELECT [Id], [YearsOld] AS [Age]
FROM [PropertyAlias]
WHERE [YearsOld] = @p1",
                    new Dictionary<string, object>
                        {
                            ["p0"] = null,
                            ["p1"] = 5
                        });

                Assert.Equal(expected, sql, SqlCommandComparer.Instance);
            }
        }

        public class MakeGetFirstNCommandFromSql
            : SqlServer2012DialectTests
        {
            [Fact]
            public void Adds_conditions_clause()
            {
                // Act
                var sql = this.config.Dialect.MakeGetFirstNCommand<Dog>(1, $"WHERE Name LIKE {"Foo%"}", "Name");

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
                // Act
                var sql = this.config.Dialect.MakeGetFirstNCommand<PropertyAlias>(1, $"WHERE Name LIKE {"Foo%"}", "Name");

                // Assert
                var expected = new SqlCommand(@"
SELECT TOP 1 [Id], [YearsOld] AS [Age]
FROM [PropertyAlias]
WHERE Name LIKE @p0
ORDER BY Name",
                    new Dictionary<string, object>
                        {
                            ["p0"] = "Foo%"
                        });

                Assert.Equal(expected, sql, SqlCommandComparer.Instance);
            }

            [Theory]
            [InlineData(null)]
            [InlineData("")]
            [InlineData(" ")]
            public void Does_not_order_when_no_orderby_given(string orderBy)
            {
                // Act
                var sql = this.config.Dialect.MakeGetFirstNCommand<Dog>(1, $"WHERE Name LIKE {"Foo%"}", orderBy);

                // Assert
                var expected = new SqlCommand(@"
SELECT TOP 1 [Id], [Name], [Age]
FROM [Dogs]
WHERE Name LIKE @p0",
                    new Dictionary<string, object>
                        {
                            ["p0"] = "Foo%"
                        });

                Assert.Equal(expected, sql, SqlCommandComparer.Instance);
            }
        }

        public class MakeGetFirstNCommandFromParameters
            : SqlServer2012DialectTests
        {
            [Fact]
            public void Selects_from_given_table()
            {
                // Act
                var sql = this.config.Dialect.MakeGetFirstNCommand<Dog>(1, new { Name = "Fido" }, "Name");

                // Assert
                var expected = new SqlCommand(@"
SELECT TOP 1 [Id], [Name], [Age]
FROM [Dogs]
WHERE [Name] = @p1
ORDER BY Name",
                    new Dictionary<string, object>
                        {
                            ["p0"] = null,
                            ["p1"] = "Fido"
                        });

                Assert.Equal(expected, sql, SqlCommandComparer.Instance);
            }

            [Fact]
            public void Adds_alias_when_column_name_is_aliased()
            {
                // Act
                var sql = this.config.Dialect.MakeGetFirstNCommand<PropertyAlias>(1, new { Age = 5 }, "Name");

                // Assert
                var expected = new SqlCommand(@"
SELECT TOP 1 [Id], [YearsOld] AS [Age]
FROM [PropertyAlias]
WHERE [YearsOld] = @p1
ORDER BY Name",
                    new Dictionary<string, object>
                        {
                            ["p0"] = null,
                            ["p1"] = 5
                        });

                Assert.Equal(expected, sql, SqlCommandComparer.Instance);
            }

            [Theory]
            [InlineData(null)]
            [InlineData("")]
            [InlineData(" ")]
            public void Does_not_order_when_no_orderby_given(string orderBy)
            {
                // Act
                var sql = this.config.Dialect.MakeGetFirstNCommand<Dog>(1, new { Name = "Fido" }, orderBy);

                // Assert
                var expected = new SqlCommand(@"
SELECT TOP 1 [Id], [Name], [Age]
FROM [Dogs]
WHERE [Name] = @p1",
                    new Dictionary<string, object>
                        {
                            ["p0"] = null,
                            ["p1"] = "Fido"
                        });

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
                // Act / Assert
                Assert.Throws<ArgumentException>(
                    () => this.config.Dialect.MakeGetPageCommand<Dog>(new Page(1, 10, true, 0, 9), null, orderBy));
            }

            [Fact]
            public void Selects_from_given_table()
            {
                // Act
                var sql = this.config.Dialect.MakeGetPageCommand<Dog>(new Page(1, 10, true, 0, 9), null, "Name");

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
                // Act
                var sql = this.config.Dialect.MakeGetPageCommand<Dog>(new Page(1, 10, true, 0, 9), $"WHERE Name LIKE {"Foo%"}", "Name");

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
                // Act
                var sql = this.config.Dialect.MakeGetPageCommand<PropertyAlias>(new Page(1, 10, true, 0, 9), null, "Name");

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
                // Act
                var sql = this.config.Dialect.MakeGetPageCommand<Dog>(new Page(2, 10, true, 10, 19), null, "Name");

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
                // Act
                var sql = this.config.Dialect.MakeGetPageCommand<Dog>(new Page(2, 5, true, 5, 9), null, "Name");

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
                // Act
                var sql = this.config.Dialect.MakeInsertCommand(new Dog { Name = "Foo", Age = 10 });

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
                // Act
                var sql = this.config.Dialect.MakeInsertCommand(new KeyNotGenerated { Id = 6, Name = "Foo" });

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
                // Act
                var sql = this.config.Dialect.MakeInsertCommand(new PropertyComputed { Name = "Foo" });

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
                // Act
                var sql = this.config.Dialect.MakeInsertCommand(new PropertyGenerated { Name = "Foo" });

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
                // Act
                var sql = this.config.Dialect.MakeInsertReturningPrimaryKeyCommand<int>(new Dog { Name = "Foo", Age = 10 });

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
                // Act
                var sql = this.config.Dialect.MakeInsertReturningPrimaryKeyCommand<int>(new KeyNotGenerated { Id = 10, Name = "Foo" });

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
                // Act
                var sql = this.config.Dialect.MakeInsertReturningPrimaryKeyCommand<int>(new PropertyComputed { Name = "Foo" });

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
                // Act
                var sql = this.config.Dialect.MakeInsertReturningPrimaryKeyCommand<int>(new PropertyGenerated { Name = "Foo" });

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
                // Act
                var sql = this.config.Dialect.MakeUpdateCommand(new Dog { Id = 5, Name = "Foo", Age = 10 });

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
                // Act
                var sql = this.config.Dialect.MakeUpdateCommand(new CompositeKeys { Key1 = 7, Key2 = 8, Name = "Foo" });

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
                // Act
                var sql = this.config.Dialect.MakeUpdateCommand(new KeyNotGenerated { Id = 7, Name = "Foo" });

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
                // Act
                var sql = this.config.Dialect.MakeUpdateCommand(new PropertyAlias { Id = 5, Age = 10 });

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
                // Act
                var sql = this.config.Dialect.MakeUpdateCommand(new KeyAlias { Name = "Foo", Id = 10 });

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
                // Act
                var sql = this.config.Dialect.MakeUpdateCommand(new KeyExplicit { Name = "Foo", Key = 10 });

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
                // Act
                var sql = this.config.Dialect.MakeUpdateCommand(new PropertyComputed { Name = "Foo", Id = 10 });

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
                // Act
                var sql = this.config.Dialect.MakeUpdateCommand(new PropertyGenerated { Id = 5, Name = "Foo", Created = new DateTime(2018, 4, 1) });

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
                // Act
                var sql = this.config.Dialect.MakeDeleteByPrimaryKeyCommand<Dog>(5);

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
                // Act
                var sql = this.config.Dialect.MakeDeleteByPrimaryKeyCommand<CompositeKeys>(new { Key1 = 1, Key2 = 2 });

                // Assert
                var expected = new SqlCommand(@"
DELETE FROM [CompositeKeys]
WHERE [Key1] = @Key1 AND [Key2] = @Key2",
                    new Dictionary<string, object>
                        {
                            ["Key1"] = 1,
                            ["Key2"] = 2
                        });

                Assert.Equal(expected, sql, SqlCommandComparer.Instance);
            }

            [Fact]
            public void Uses_primary_key_even_if_its_not_auto_generated()
            {
                // Act
                var sql = this.config.Dialect.MakeDeleteByPrimaryKeyCommand<KeyNotGenerated>(5);

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
                // Act
                var sql = this.config.Dialect.MakeDeleteByPrimaryKeyCommand<KeyAlias>(5);

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
                // Act
                var sql = this.config.Dialect.MakeDeleteByPrimaryKeyCommand<KeyExplicit>(5);

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
                // Act
                var sql = this.config.Dialect.MakeDeleteRangeCommand<Dog>($"WHERE [Age] > {10}");

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
                Assert.Throws<ArgumentException>(() => this.config.Dialect.MakeCreateTempTableCommand<Dog>());
            }

            [Fact]
            public void Throws_exception_if_there_are_no_columns()
            {
                // Act
                Assert.Throws<ArgumentException>(() => this.config.Dialect.MakeCreateTempTableCommand<NoColumns>());
            }
        }
    }
}