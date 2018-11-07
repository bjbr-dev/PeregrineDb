namespace PeregrineDb.Tests.Dialects
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Diagnostics.CodeAnalysis;
    using FluentAssertions;
    using Moq;
    using Pagination;
    using PeregrineDb;
    using PeregrineDb.Dialects;
    using PeregrineDb.Dialects.SqlServer2012;
    using PeregrineDb.Schema;
    using PeregrineDb.Tests.ExampleEntities;
    using PeregrineDb.Tests.Utils;
    using Xunit;

    [SuppressMessage("ReSharper", "ConvertToConstant.Local")]
    public class SqlServer2012DialectTests
    {
        private PeregrineConfig config = PeregrineConfig.SqlServer2012;

        private IDialect Sut => this.config.Dialect;

        private TableSchema GetTableSchema<T>()
        {
            return this.config.SchemaFactory.GetTableSchema(typeof(T));
        }

        public class MakeCountStatementFromSql
            : SqlServer2012DialectTests
        {
            [Fact]
            public void Selects_from_given_table()
            {
                // Act
                var command = this.Sut.MakeCountCommand<Dog>(null, null);

                // Assert
                var expected = new SqlCommand(@"
SELECT COUNT(*)
FROM [Dogs]");

                command.Should().Be(expected);
            }

            [Fact]
            public void Adds_conditions()
            {
                // Act
                var command = this.Sut.MakeCountCommand<Dog>("WHERE Foo IS NOT NULL", null);

                // Assert
                var expected = new SqlCommand(@"
SELECT COUNT(*)
FROM [Dogs]
WHERE Foo IS NOT NULL");

                command.Should().Be(expected);
            }
        }
        public class MakeCountStatementFromParameters
            : SqlServer2012DialectTests
        {
            [Fact]
            public void Errors_when_conditions_is_null()
            {
                // Act
                Action act = () => this.Sut.MakeCountCommand<Dog>(null);

                // Assert
                act.Should().Throw<ArgumentNullException>();
            }

            [Fact]
            public void Adds_conditions()
            {
                // Act
                var command = this.Sut.MakeCountCommand<Dog>(new { Name = (string)null });

                // Assert
                var expected = new SqlCommand(@"
SELECT COUNT(*)
FROM [Dogs]
WHERE [Name] IS NULL",
                    new { Name = (string)null });

                command.Should().Be(expected);
            }
        }

        public class MakeFindStatement
            : SqlServer2012DialectTests
        {
            [Fact]
            public void Errors_when_id_is_null()
            {
                // Act
                Action act = () => this.Sut.MakeFindCommand<Dog>(null);

                // Assert
                act.Should().Throw<ArgumentNullException>();
            }

            [Fact]
            public void Selects_from_given_table()
            {
                // Act
                var command = this.Sut.MakeFindCommand<Dog>(5);

                // Assert
                var expected = new SqlCommand(@"
SELECT [Id], [Name], [Age]
FROM [Dogs]
WHERE [Id] = @Id",
                    new Dictionary<string, object>
                        {
                            ["Id"] = 5
                        });

                command.Should().Be(expected);
            }

            [Fact]
            public void Uses_non_default_primary_key_name()
            {
                // Act
                var command = this.Sut.MakeFindCommand<KeyExplicit>(5);

                // Assert
                var expected = new SqlCommand(@"
SELECT [Key], [Name]
FROM [KeyExplicit]
WHERE [Key] = @Key",
                    new Dictionary<string, object>
                        {
                            ["Key"] = 5
                        });

                command.Should().Be(expected);
            }

            [Fact]
            public void Uses_each_key_in_composite_key()
            {
                // Act
                var command = this.Sut.MakeFindCommand<CompositeKeys>(new { key1 = 2, key2 = 3 });

                // Assert
                var expected = new SqlCommand(@"
SELECT [Key1], [Key2], [Name]
FROM [CompositeKeys]
WHERE [Key1] = @Key1 AND [Key2] = @Key2",
                    new { key1 = 2, key2 = 3 });

                command.Should().Be(expected);
            }

            [Fact]
            public void Adds_alias_when_primary_key_is_aliased()
            {
                // Act
                var command = this.Sut.MakeFindCommand<KeyAlias>(5);

                // Assert
                var expected = new SqlCommand(@"
SELECT [Key] AS [Id], [Name]
FROM [KeyAlias]
WHERE [Key] = @Id",
                    new Dictionary<string, object>
                        {
                            ["Id"] = 5
                        });

                command.Should().Be(expected);
            }

            [Fact]
            public void Adds_alias_when_column_name_is_aliased()
            {
                // Act
                var command = this.Sut.MakeFindCommand<PropertyAlias>(5);

                // Assert
                var expected = new SqlCommand(@"
SELECT [Id], [YearsOld] AS [Age]
FROM [PropertyAlias]
WHERE [Id] = @Id",
                    new Dictionary<string, object>
                        {
                            ["Id"] = 5
                        });

                command.Should().Be(expected);
            }
        }

        public class MakeGetRangeStatementFromSql
            : SqlServer2012DialectTests
        {
            [Fact]
            public void Selects_from_given_table()
            {
                // Act
                var command = this.Sut.MakeGetRangeCommand<Dog>(null, null);

                // Assert
                var expected = new SqlCommand(@"
SELECT [Id], [Name], [Age]
FROM [Dogs]");

                command.Should().Be(expected);
            }

            [Fact]
            public void Adds_conditions_clause()
            {
                // Act
                var command = this.Sut.MakeGetRangeCommand<Dog>("WHERE Age > @Age", new { Age = 10 });

                // Assert
                var expected = new SqlCommand(@"
SELECT [Id], [Name], [Age]
FROM [Dogs]
WHERE Age > @Age",
                    new { Age = 10 });

                command.Should().Be(expected);
            }

            [Fact]
            public void Uses_explicit_primary_key_name()
            {
                // Act
                var command = this.Sut.MakeGetRangeCommand<KeyExplicit>(null, null);

                // Assert
                var expected = new SqlCommand(@"
SELECT [Key], [Name]
FROM [KeyExplicit]");

                command.Should().Be(expected);
            }

            [Fact]
            public void Adds_alias_when_primary_key_is_aliased()
            {
                // Act
                var command = this.Sut.MakeGetRangeCommand<KeyAlias>(null, null);

                // Assert
                var expected = new SqlCommand(@"
SELECT [Key] AS [Id], [Name]
FROM [KeyAlias]");

                command.Should().Be(expected);
            }

            [Fact]
            public void Adds_alias_when_column_name_is_aliased()
            {
                // Act
                var command = this.Sut.MakeGetRangeCommand<PropertyAlias>(null, null);

                // Assert
                var expected = new SqlCommand(@"
SELECT [Id], [YearsOld] AS [Age]
FROM [PropertyAlias]");

                command.Should().Be(expected);
            }
        }

        public class MakeGetRangeStatementFromParameters
            : SqlServer2012DialectTests
        {
            [Fact]
            public void Selects_from_given_table()
            {
                // Act
                var command = this.Sut.MakeGetRangeCommand<Dog>(new { Name = "Fido" });

                // Assert
                var expected = new SqlCommand(@"
SELECT [Id], [Name], [Age]
FROM [Dogs]
WHERE [Name] = @Name",
                    new { Name = "Fido" });

                command.Should().Be(expected);
            }

            [Fact]
            public void Uses_explicit_primary_key_name()
            {
                // Act
                var command = this.Sut.MakeGetRangeCommand<KeyExplicit>(new { Name = "Fido" });

                // Assert
                var expected = new SqlCommand(@"
SELECT [Key], [Name]
FROM [KeyExplicit]
WHERE [Name] = @Name",
                    new { Name = "Fido" });

                command.Should().Be(expected);
            }

            [Fact]
            public void Adds_alias_when_primary_key_is_aliased()
            {
                // Act
                var command = this.Sut.MakeGetRangeCommand<KeyAlias>(new { Name = "Fido" });

                // Assert
                var expected = new SqlCommand(@"
SELECT [Key] AS [Id], [Name]
FROM [KeyAlias]
WHERE [Name] = @Name",
                    new { Name = "Fido" });

                command.Should().Be(expected);
            }

            [Fact]
            public void Adds_alias_when_column_name_is_aliased()
            {
                // Act
                var command = this.Sut.MakeGetRangeCommand<PropertyAlias>(new { Age = 5 });

                // Assert
                var expected = new SqlCommand(@"
SELECT [Id], [YearsOld] AS [Age]
FROM [PropertyAlias]
WHERE [YearsOld] = @Age",
                    new { Age = 5 });

                command.Should().Be(expected);
            }
        }

        public class MakeGetFirstNCommandFromSql
            : SqlServer2012DialectTests
        {
            [Fact]
            public void Adds_conditions_clause()
            {
                // Act
                var command = this.Sut.MakeGetFirstNCommand<Dog>(1, "WHERE Name LIKE @Name", new { Name = "Foo%" }, "Name");

                // Assert
                var expected = new SqlCommand(@"
SELECT TOP 1 [Id], [Name], [Age]
FROM [Dogs]
WHERE Name LIKE @Name
ORDER BY Name",
                    new { Name = "Foo%" });

                command.Should().Be(expected);
            }

            [Fact]
            public void Adds_alias_when_column_name_is_aliased()
            {
                // Act
                var command = this.Sut.MakeGetFirstNCommand<PropertyAlias>(1, "WHERE Name LIKE @p0", new { p0 = "Foo%" }, "Name");

                // Assert
                var expected = new SqlCommand(@"
SELECT TOP 1 [Id], [YearsOld] AS [Age]
FROM [PropertyAlias]
WHERE Name LIKE @p0
ORDER BY Name",
                    new { p0 = "Foo%" });

                command.Should().Be(expected);
            }

            [Theory]
            [InlineData(null)]
            [InlineData("")]
            [InlineData(" ")]
            public void Does_not_order_when_no_orderby_given(string orderBy)
            {
                // Act
                var command = this.Sut.MakeGetFirstNCommand<Dog>(1, "WHERE Name LIKE @p0", new { p0 = "Foo%" }, orderBy);

                // Assert
                var expected = new SqlCommand(@"
SELECT TOP 1 [Id], [Name], [Age]
FROM [Dogs]
WHERE Name LIKE @p0",
                    new { p0 = "Foo%" });

                command.Should().Be(expected);
            }
        }

        public class MakeGetFirstNCommandFromParameters
            : SqlServer2012DialectTests
        {
            [Fact]
            public void Selects_from_given_table()
            {
                // Act
                var command = this.Sut.MakeGetFirstNCommand<Dog>(1, new { Name = "Fido" }, "Name");

                // Assert
                var expected = new SqlCommand(@"
SELECT TOP 1 [Id], [Name], [Age]
FROM [Dogs]
WHERE [Name] = @Name
ORDER BY Name",
                    new { Name = "Fido" });

                command.Should().Be(expected);
            }

            [Fact]
            public void Adds_alias_when_column_name_is_aliased()
            {
                // Act
                var command = this.Sut.MakeGetFirstNCommand<PropertyAlias>(1, new { Age = 5 }, "Name");

                // Assert
                var expected = new SqlCommand(@"
SELECT TOP 1 [Id], [YearsOld] AS [Age]
FROM [PropertyAlias]
WHERE [YearsOld] = @Age
ORDER BY Name",
                    new { Age = 5 });

                command.Should().Be(expected);
            }

            [Theory]
            [InlineData(null)]
            [InlineData("")]
            [InlineData(" ")]
            public void Does_not_order_when_no_orderby_given(string orderBy)
            {
                // Act
                var command = this.Sut.MakeGetFirstNCommand<Dog>(1, new { Name = "Fido" }, orderBy);

                // Assert
                var expected = new SqlCommand(@"
SELECT TOP 1 [Id], [Name], [Age]
FROM [Dogs]
WHERE [Name] = @Name",
                    new { Name = "Fido" });

                command.Should().Be(expected);
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
                    () => this.Sut.MakeGetPageCommand<Dog>(new Page(1, 10, true, 0, 9), null, null, orderBy));
            }

            [Fact]
            public void Selects_from_given_table()
            {
                // Act
                var command = this.Sut.MakeGetPageCommand<Dog>(new Page(1, 10, true, 0, 9), null, null, "Name");

                // Assert
                var expected = new SqlCommand(@"
SELECT [Id], [Name], [Age]
FROM [Dogs]
ORDER BY Name
OFFSET 0 ROWS FETCH NEXT 10 ROWS ONLY");

                command.Should().Be(expected);
            }

            [Fact]
            public void Adds_conditions_clause()
            {
                // Act
                var command = this.Sut.MakeGetPageCommand<Dog>(new Page(1, 10, true, 0, 9), "WHERE Name LIKE @Name", new { Name = "Foo%" }, "Name");

                // Assert
                var expected = new SqlCommand(@"
SELECT [Id], [Name], [Age]
FROM [Dogs]
WHERE Name LIKE @Name
ORDER BY Name
OFFSET 0 ROWS FETCH NEXT 10 ROWS ONLY",
                    new { Name = "Foo%" });

                command.Should().Be(expected);
            }

            [Fact]
            public void Adds_alias_when_column_name_is_aliased()
            {
                // Act
                var command = this.Sut.MakeGetPageCommand<PropertyAlias>(new Page(1, 10, true, 0, 9), null, null, "Name");

                // Assert
                var expected = new SqlCommand(@"
SELECT [Id], [YearsOld] AS [Age]
FROM [PropertyAlias]
ORDER BY Name
OFFSET 0 ROWS FETCH NEXT 10 ROWS ONLY");

                command.Should().Be(expected);
            }

            [Fact]
            public void Selects_second_page()
            {
                // Act
                var command = this.Sut.MakeGetPageCommand<Dog>(new Page(2, 10, true, 10, 19), null, null, "Name");

                // Assert
                var expected = new SqlCommand(@"
SELECT [Id], [Name], [Age]
FROM [Dogs]
ORDER BY Name
OFFSET 10 ROWS FETCH NEXT 10 ROWS ONLY");

                command.Should().Be(expected);
            }

            [Fact]
            public void Selects_appropriate_number_of_rows()
            {
                // Act
                var command = this.Sut.MakeGetPageCommand<Dog>(new Page(2, 5, true, 5, 9), null, null, "Name");

                // Assert
                var expected = new SqlCommand(@"
SELECT [Id], [Name], [Age]
FROM [Dogs]
ORDER BY Name
OFFSET 5 ROWS FETCH NEXT 5 ROWS ONLY");

                command.Should().Be(expected);
            }
        }

        public class MakeInsertStatement
            : SqlServer2012DialectTests
        {
            [Fact]
            public void Inserts_into_given_table()
            {
                // Act
                var command = this.Sut.MakeInsertCommand(new Dog { Name = "Foo", Age = 10 });

                // Assert
                var expected = new SqlCommand(@"
INSERT INTO [Dogs] ([Name], [Age])
VALUES (@Name, @Age);",
                    new Dog { Name = "Foo", Age = 10 });

                command.Should().Be(expected);
            }

            [Fact]
            public void Adds_primary_key_if_its_not_generated_by_database()
            {
                // Act
                var command = this.Sut.MakeInsertCommand(new KeyNotGenerated { Id = 6, Name = "Foo" });

                // Assert
                var expected = new SqlCommand(@"
INSERT INTO [KeyNotGenerated] ([Id], [Name])
VALUES (@Id, @Name);",
                    new KeyNotGenerated { Id = 6, Name = "Foo" });

                command.Should().Be(expected);
            }

            [Fact]
            public void Does_not_include_computed_columns()
            {
                // Act
                var command = this.Sut.MakeInsertCommand(new PropertyComputed { Name = "Foo" });

                // Assert
                var expected = new SqlCommand(@"
INSERT INTO [PropertyComputed] ([Name])
VALUES (@Name);",
                    new PropertyComputed { Name = "Foo" });

                command.Should().Be(expected);
            }

            [Fact]
            public void Does_not_include_generated_columns()
            {
                // Act
                var command = this.Sut.MakeInsertCommand(new PropertyGenerated { Name = "Foo" });

                // Assert
                var expected = new SqlCommand(@"
INSERT INTO [PropertyGenerated] ([Name])
VALUES (@Name);",
                    new PropertyGenerated { Name = "Foo" });

                command.Should().Be(expected);
            }
        }

        public class MakeInsertReturningIdentityStatement
            : SqlServer2012DialectTests
        {
            [Fact]
            public void Inserts_into_given_table()
            {
                // Act
                var command = this.Sut.MakeInsertReturningPrimaryKeyCommand<int>(new Dog { Name = "Foo", Age = 10 }, this.GetTableSchema<Dog>());

                // Assert
                var expected = new SqlCommand(@"
INSERT INTO [Dogs] ([Name], [Age])
VALUES (@Name, @Age);
SELECT CAST(SCOPE_IDENTITY() AS BIGINT) AS [id]",
                    new Dog { Name = "Foo", Age = 10 });

                command.Should().Be(expected);
            }

            [Fact]
            public void Adds_primary_key_if_its_not_generated_by_database()
            {
                // Act
                var command = this.Sut.MakeInsertReturningPrimaryKeyCommand<int>(new KeyNotGenerated { Id = 10, Name = "Foo" }, this.GetTableSchema<KeyNotGenerated>());

                // Assert
                var expected = new SqlCommand(@"
INSERT INTO [KeyNotGenerated] ([Id], [Name])
VALUES (@Id, @Name);
SELECT CAST(SCOPE_IDENTITY() AS BIGINT) AS [id]",
                    new KeyNotGenerated { Id = 10, Name = "Foo" });

                command.Should().Be(expected);
            }

            [Fact]
            public void Does_not_include_computed_columns()
            {
                // Act
                var command = this.Sut.MakeInsertReturningPrimaryKeyCommand<int>(new PropertyComputed { Name = "Foo" }, this.GetTableSchema<PropertyComputed>());

                // Assert
                var expected = new SqlCommand(@"
INSERT INTO [PropertyComputed] ([Name])
VALUES (@Name);
SELECT CAST(SCOPE_IDENTITY() AS BIGINT) AS [id]",
                    new PropertyComputed { Name = "Foo" });

                command.Should().Be(expected);
            }

            [Fact]
            public void Does_not_include_generated_columns()
            {
                // Act
                var command = this.Sut.MakeInsertReturningPrimaryKeyCommand<int>(new PropertyGenerated { Name = "Foo" }, this.GetTableSchema<PropertyGenerated>());

                // Assert
                var expected = new SqlCommand(@"
INSERT INTO [PropertyGenerated] ([Name])
VALUES (@Name);
SELECT CAST(SCOPE_IDENTITY() AS BIGINT) AS [id]",
                    new PropertyGenerated { Name = "Foo" });

                command.Should().Be(expected);
            }
        }

        public class MakeUpdateStatement
            : SqlServer2012DialectTests
        {
            [Fact]
            public void Updates_given_table()
            {
                // Act
                var command = this.Sut.MakeUpdateCommand(new Dog { Id = 5, Name = "Foo", Age = 10 });

                // Assert
                var expected = new SqlCommand(@"
UPDATE [Dogs]
SET [Name] = @Name, [Age] = @Age
WHERE [Id] = @Id",
                    new Dog { Id = 5, Name = "Foo", Age = 10 });

                command.Should().Be(expected);
            }

            [Fact]
            public void Uses_each_key_in_composite_key()
            {
                // Act
                var command = this.Sut.MakeUpdateCommand(new CompositeKeys { Key1 = 7, Key2 = 8, Name = "Foo" });

                // Assert
                var expected = new SqlCommand(@"
UPDATE [CompositeKeys]
SET [Name] = @Name
WHERE [Key1] = @Key1 AND [Key2] = @Key2",
                    new CompositeKeys { Key1 = 7, Key2 = 8, Name = "Foo" });

                command.Should().Be(expected);
            }

            [Fact]
            public void Does_not_update_primary_key_even_if_its_not_auto_generated()
            {
                // Act
                var command = this.Sut.MakeUpdateCommand(new KeyNotGenerated { Id = 7, Name = "Foo" });

                // Assert
                var expected = new SqlCommand(@"
UPDATE [KeyNotGenerated]
SET [Name] = @Name
WHERE [Id] = @Id",
                    new KeyNotGenerated { Id = 7, Name = "Foo" });

                command.Should().Be(expected);
            }

            [Fact]
            public void Uses_aliased_property_names()
            {
                // Act
                var command = this.Sut.MakeUpdateCommand(new PropertyAlias { Id = 5, Age = 10 });

                // Assert
                var expected = new SqlCommand(@"
UPDATE [PropertyAlias]
SET [YearsOld] = @Age
WHERE [Id] = @Id",
                    new PropertyAlias { Id = 5, Age = 10 });

                command.Should().Be(expected);
            }

            [Fact]
            public void Uses_aliased_key_name()
            {
                // Act
                var command = this.Sut.MakeUpdateCommand(new KeyAlias { Name = "Foo", Id = 10 });

                // Assert
                var expected = new SqlCommand(@"
UPDATE [KeyAlias]
SET [Name] = @Name
WHERE [Key] = @Id",
                    new KeyAlias { Name = "Foo", Id = 10 });

                command.Should().Be(expected);
            }

            [Fact]
            public void Uses_explicit_key_name()
            {
                // Act
                var command = this.Sut.MakeUpdateCommand(new KeyExplicit { Name = "Foo", Key = 10 });

                // Assert
                var expected = new SqlCommand(@"
UPDATE [KeyExplicit]
SET [Name] = @Name
WHERE [Key] = @Key",
                    new KeyExplicit
                        {
                            Key = 10,
                            Name = "Foo"
                        });

                command.Should().Be(expected);
            }

            [Fact]
            public void Does_not_include_computed_columns()
            {
                // Act
                var command = this.Sut.MakeUpdateCommand(new PropertyComputed { Name = "Foo", Id = 10 });

                // Assert
                var expected = new SqlCommand(@"
UPDATE [PropertyComputed]
SET [Name] = @Name
WHERE [Id] = @Id",
                    new PropertyComputed { Name = "Foo", Id = 10 });

                command.Should().Be(expected);
            }

            [Fact]
            public void Includes_generated_columns()
            {
                // Act
                var command = this.Sut.MakeUpdateCommand(new PropertyGenerated { Id = 5, Name = "Foo", Created = new DateTime(2018, 4, 1) });

                // Assert
                var expected = new SqlCommand(@"
UPDATE [PropertyGenerated]
SET [Name] = @Name, [Created] = @Created
WHERE [Id] = @Id",
                    new PropertyGenerated { Id = 5, Name = "Foo", Created = new DateTime(2018, 4, 1) });

                command.Should().Be(expected);
            }
        }

        public class MakeDeleteByPrimaryKeyStatement
            : SqlServer2012DialectTests
        {
            [Fact]
            public void Deletes_from_given_table()
            {
                // Act
                var command = this.Sut.MakeDeleteByPrimaryKeyCommand<Dog>(5);

                // Assert
                var expected = new SqlCommand(@"
DELETE FROM [Dogs]
WHERE [Id] = @Id",
                    new Dictionary<string, object>
                        {
                            ["Id"] = 5
                        });

                command.Should().Be(expected);
            }

            [Fact]
            public void Uses_each_key_in_composite_key()
            {
                // Act
                var command = this.Sut.MakeDeleteByPrimaryKeyCommand<CompositeKeys>(new { Key1 = 1, Key2 = 2 });

                // Assert
                var expected = new SqlCommand(@"
DELETE FROM [CompositeKeys]
WHERE [Key1] = @Key1 AND [Key2] = @Key2",
                    new { Key1 = 1, Key2 = 2 });

                command.Should().Be(expected);
            }

            [Fact]
            public void Uses_primary_key_even_if_its_not_auto_generated()
            {
                // Act
                var command = this.Sut.MakeDeleteByPrimaryKeyCommand<KeyNotGenerated>(5);

                // Assert
                var expected = new SqlCommand(@"
DELETE FROM [KeyNotGenerated]
WHERE [Id] = @Id",
                    new Dictionary<string, object>
                        {
                            ["Id"] = 5
                        });

                command.Should().Be(expected);
            }

            [Fact]
            public void Uses_aliased_key_name()
            {
                // Act
                var command = this.Sut.MakeDeleteByPrimaryKeyCommand<KeyAlias>(5);

                // Assert
                var expected = new SqlCommand(@"
DELETE FROM [KeyAlias]
WHERE [Key] = @Id",
                    new Dictionary<string, object>
                        {
                            ["Id"] = 5
                        });

                command.Should().Be(expected);
            }

            [Fact]
            public void Uses_explicit_key_name()
            {
                // Act
                var command = this.Sut.MakeDeleteByPrimaryKeyCommand<KeyExplicit>(5);

                // Assert
                var expected = new SqlCommand(@"
DELETE FROM [KeyExplicit]
WHERE [Key] = @Key",
                    new Dictionary<string, object>
                        {
                            ["Key"] = 5
                        });

                command.Should().Be(expected);
            }
        }

        public class MakeDeleteRangeStatement
            : SqlServer2012DialectTests
        {
            [Fact]
            public void Deletes_from_given_table()
            {
                // Act
                var command = this.Sut.MakeDeleteRangeCommand<Dog>("WHERE [Age] > @Age", new { Age = 10 });

                // Assert
                var expected = new SqlCommand(@"
DELETE FROM [Dogs]
WHERE [Age] > @Age",
                    new { Age = 10 });

                command.Should().Be(expected);
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
                Assert.Throws<ArgumentException>(() => this.Sut.MakeCreateTempTableCommand<Dog>());
            }

            [Fact]
            public void Throws_exception_if_there_are_no_columns()
            {
                // Act
                Assert.Throws<ArgumentException>(() => this.Sut.MakeCreateTempTableCommand<NoColumns>());
            }
        }
    }
}