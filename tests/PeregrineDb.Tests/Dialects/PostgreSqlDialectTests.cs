namespace PeregrineDb.Tests.Dialects
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Diagnostics.CodeAnalysis;
    using FluentAssertions;
    using Pagination;
    using PeregrineDb;
    using PeregrineDb.Dialects;
    using PeregrineDb.Schema;
    using PeregrineDb.Tests.ExampleEntities;
    using PeregrineDb.Tests.Utils;
    using Xunit;

    [SuppressMessage("ReSharper", "ConvertToConstant.Local")]
    public class PostgreSqlDialectTests
    {
        private PeregrineConfig config = PeregrineConfig.Postgres;

        private IDialect Sut => this.config.Dialect;

        private TableSchema GetTableSchema<T>()
        {
            return this.config.SchemaFactory.GetTableSchema(typeof(T));
        }

        public class MakeCountStatementFromSql
            : PostgreSqlDialectTests
        {
            [Fact]
            public void Selects_from_given_table()
            {
                // Act
                var command = this.Sut.MakeCountCommand<Dog>(null, null);

                // Assert
                var expected = new SqlCommand(@"
SELECT COUNT(*)
FROM dog");

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
FROM dog
WHERE Foo IS NOT NULL");

                command.Should().Be(expected);
            }
        }
        public class MakeCountStatementFromParameters
            : PostgreSqlDialectTests
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
FROM dog
WHERE name IS NULL",
                    new { Name = (string)null });

                command.Should().Be(expected);
            }
        }

        public class MakeFindStatement
            : PostgreSqlDialectTests
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
SELECT id, name, age
FROM dog
WHERE id = @Id",
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
SELECT key, name
FROM KeyExplicit
WHERE key = @Key",
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
SELECT key1, key2, name
FROM CompositeKeys
WHERE key1 = @Key1 AND key2 = @Key2",
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
SELECT Key AS Id, name
FROM KeyAlias
WHERE Key = @Id",
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
SELECT id, YearsOld AS Age
FROM PropertyAlias
WHERE id = @Id",
                    new Dictionary<string, object>
                    {
                        ["Id"] = 5
                    });

                command.Should().Be(expected);
            }
        }

        public class MakeGetRangeStatementFromSql
            : PostgreSqlDialectTests
        {
            [Fact]
            public void Selects_from_given_table()
            {
                // Act
                var command = this.Sut.MakeGetRangeCommand<Dog>(null, null);

                // Assert
                var expected = new SqlCommand(@"
SELECT id, name, age
FROM dog");

                command.Should().Be(expected);
            }

            [Fact]
            public void Adds_conditions_clause()
            {
                // Act
                var command = this.Sut.MakeGetRangeCommand<Dog>("WHERE Age > @Age", new { Age = 10 });

                // Assert
                var expected = new SqlCommand(@"
SELECT id, name, age
FROM dog
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
SELECT key, name
FROM KeyExplicit");

                command.Should().Be(expected);
            }

            [Fact]
            public void Adds_alias_when_primary_key_is_aliased()
            {
                // Act
                var command = this.Sut.MakeGetRangeCommand<KeyAlias>(null, null);

                // Assert
                var expected = new SqlCommand(@"
SELECT Key AS Id, name
FROM KeyAlias");

                command.Should().Be(expected);
            }

            [Fact]
            public void Adds_alias_when_column_name_is_aliased()
            {
                // Act
                var command = this.Sut.MakeGetRangeCommand<PropertyAlias>(null, null);

                // Assert
                var expected = new SqlCommand(@"
SELECT id, YearsOld AS Age
FROM PropertyAlias");

                command.Should().Be(expected);
            }
        }

        public class MakeGetRangeStatementFromParameters
            : PostgreSqlDialectTests
        {
            [Fact]
            public void Selects_from_given_table()
            {
                // Act
                var command = this.Sut.MakeGetRangeCommand<Dog>(new { Name = "Fido" });

                // Assert
                var expected = new SqlCommand(@"
SELECT id, name, age
FROM dog
WHERE name = @Name",
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
SELECT key, name
FROM KeyExplicit
WHERE name = @Name",
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
SELECT Key AS Id, name
FROM KeyAlias
WHERE name = @Name",
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
SELECT id, YearsOld AS Age
FROM PropertyAlias
WHERE YearsOld = @Age",
                    new { Age = 5 });

                command.Should().Be(expected);
            }
        }

        public class MakeGetFirstNCommandFromSql
            : PostgreSqlDialectTests
        {
            [Fact]
            public void Adds_conditions_clause()
            {
                // Act
                var command = this.Sut.MakeGetFirstNCommand<Dog>(1, "WHERE Name LIKE @Name", new { Name = "Foo%" }, "Name");

                // Assert
                var expected = new SqlCommand(@"
SELECT id, name, age
FROM dog
WHERE Name LIKE @Name
ORDER BY Name
LIMIT 1",
                    new { Name = "Foo%" });

                command.Should().Be(expected);
            }

            [Fact]
            public void Adds_alias_when_column_name_is_aliased()
            {
                // Act
                var command = this.Sut.MakeGetFirstNCommand<PropertyAlias>(1, "WHERE Name LIKE @Name", new { Name = "Foo%" }, "Name");

                // Assert
                var expected = new SqlCommand(@"
SELECT id, YearsOld AS Age
FROM PropertyAlias
WHERE Name LIKE @Name
ORDER BY Name
LIMIT 1",
                    new { Name = "Foo%" });

                command.Should().Be(expected);
            }

            [Theory]
            [InlineData(null)]
            [InlineData("")]
            [InlineData(" ")]
            public void Does_not_order_when_no_orderby_given(string orderBy)
            {
                // Act
                var command = this.Sut.MakeGetFirstNCommand<Dog>(1, "WHERE Name LIKE @Name", new { Name = "Foo%" }, orderBy);

                // Assert
                var expected = new SqlCommand(@"
SELECT id, name, age
FROM dog
WHERE Name LIKE @Name
LIMIT 1",
                    new { Name = "Foo%" });

                command.Should().Be(expected);
            }
        }

        public class MakeGetFirstNCommandFromParameters
            : PostgreSqlDialectTests
        {
            [Fact]
            public void Selects_from_given_table()
            {
                // Act
                var command = this.Sut.MakeGetFirstNCommand<Dog>(1, new { Name = "Fido" }, "Name");

                // Assert
                var expected = new SqlCommand(@"
SELECT id, name, age
FROM dog
WHERE name = @Name
ORDER BY Name
LIMIT 1",
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
SELECT id, YearsOld AS Age
FROM PropertyAlias
WHERE YearsOld = @Age
ORDER BY Name
LIMIT 1",
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
SELECT id, name, age
FROM dog
WHERE name = @Name
LIMIT 1",
                    new { Name = "Fido" });

                command.Should().Be(expected);
            }
        }

        public class MakeGetPageStatement
            : PostgreSqlDialectTests
        {
            [Theory]
            [InlineData(null)]
            [InlineData("")]
            [InlineData(" ")]
            public void Throws_exception_when_order_by_is_empty(string orderBy)
            {
                // Act / Assert
                Assert.Throws<ArgumentException>(
                    () => this.Sut.MakeGetPageCommand<Dog>(new Page(1, 10, true, 0, 9), null, orderBy));
            }

            [Fact]
            public void Selects_from_given_table()
            {
                // Act
                var command = this.Sut.MakeGetPageCommand<Dog>(new Page(1, 10, true, 0, 9), null, null, "Name");

                // Assert
                var expected = new SqlCommand(@"
SELECT id, name, age
FROM dog
ORDER BY Name
LIMIT 10 OFFSET 0");

                command.Should().Be(expected);
            }

            [Fact]
            public void Adds_conditions_clause()
            {
                // Act
                var command = this.Sut.MakeGetPageCommand<Dog>(new Page(1, 10, true, 0, 9), "WHERE Name LIKE @Name", new { Name = "Foo%" }, "Name");

                // Assert
                var expected = new SqlCommand(@"
SELECT id, name, age
FROM dog
WHERE Name LIKE @Name
ORDER BY Name
LIMIT 10 OFFSET 0",
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
SELECT id, YearsOld AS Age
FROM PropertyAlias
ORDER BY Name
LIMIT 10 OFFSET 0");

                command.Should().Be(expected);
            }

            [Fact]
            public void Selects_second_page()
            {
                // Act
                var command = this.Sut.MakeGetPageCommand<Dog>(new Page(2, 10, true, 10, 19), null, null, "Name");

                // Assert
                var expected = new SqlCommand(@"
SELECT id, name, age
FROM dog
ORDER BY Name
LIMIT 10 OFFSET 10");

                command.Should().Be(expected);
            }

            [Fact]
            public void Selects_appropriate_number_of_rows()
            {
                // Act
                var command = this.Sut.MakeGetPageCommand<Dog>(new Page(2, 5, true, 5, 9), null, null, "Name");

                // Assert
                var expected = new SqlCommand(@"
SELECT id, name, age
FROM dog
ORDER BY Name
LIMIT 5 OFFSET 5");

                command.Should().Be(expected);
            }
        }

        public class MakeInsertStatement
            : PostgreSqlDialectTests
        {
            [Fact]
            public void Inserts_into_given_table()
            {
                // Act
                var command = this.Sut.MakeInsertCommand(new Dog { Name = "Foo", Age = 10 });

                // Assert
                var expected = new SqlCommand(@"
INSERT INTO dog (name, age)
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
INSERT INTO KeyNotGenerated (id, name)
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
INSERT INTO PropertyComputed (name)
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
INSERT INTO PropertyGenerated (name)
VALUES (@Name);",
                    new PropertyGenerated { Name = "Foo" });

                command.Should().Be(expected);
            }
        }

        public class MakeInsertReturningIdentityStatement
            : PostgreSqlDialectTests
        {
            [Fact]
            public void Inserts_into_given_table()
            {
                // Act
                var command = this.Sut.MakeInsertReturningPrimaryKeyCommand<int>(new Dog { Name = "Foo", Age = 10 }, this.GetTableSchema<Dog>());

                // Assert
                var expected = new SqlCommand(@"
INSERT INTO dog (name, age)
VALUES (@Name, @Age)
RETURNING id",
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
INSERT INTO KeyNotGenerated (id, name)
VALUES (@Id, @Name)
RETURNING id",
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
INSERT INTO PropertyComputed (name)
VALUES (@Name)
RETURNING id",
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
INSERT INTO PropertyGenerated (name)
VALUES (@Name)
RETURNING id",
                    new PropertyGenerated { Name = "Foo" });

                command.Should().Be(expected);
            }
        }

        public class MakeUpdateStatement
            : PostgreSqlDialectTests
        {
            [Fact]
            public void Updates_given_table()
            {
                // Act
                var command = this.Sut.MakeUpdateCommand(new Dog { Id = 5, Name = "Foo", Age = 10 });

                // Assert
                var expected = new SqlCommand(@"
UPDATE dog
SET name = @Name, age = @Age
WHERE id = @Id",
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
UPDATE CompositeKeys
SET name = @Name
WHERE key1 = @Key1 AND key2 = @Key2",
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
UPDATE KeyNotGenerated
SET name = @Name
WHERE id = @Id",
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
UPDATE PropertyAlias
SET YearsOld = @Age
WHERE id = @Id",
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
UPDATE KeyAlias
SET name = @Name
WHERE Key = @Id",
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
UPDATE KeyExplicit
SET name = @Name
WHERE key = @Key",
                    new KeyExplicit { Name = "Foo", Key = 10 });

                command.Should().Be(expected);
            }

            [Fact]
            public void Does_not_include_computed_columns()
            {
                // Act
                var command = this.Sut.MakeUpdateCommand(new PropertyComputed { Name = "Foo", Id = 10 });

                // Assert
                var expected = new SqlCommand(@"
UPDATE PropertyComputed
SET name = @Name
WHERE id = @Id",
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
UPDATE PropertyGenerated
SET name = @Name, created = @Created
WHERE id = @Id",
                    new PropertyGenerated { Id = 5, Name = "Foo", Created = new DateTime(2018, 4, 1) });

                command.Should().Be(expected);
            }
        }

        public class MakeDeleteByPrimaryKeyStatement
            : PostgreSqlDialectTests
        {
            [Fact]
            public void Deletes_from_given_table()
            {
                // Act
                var command = this.Sut.MakeDeleteByPrimaryKeyCommand<Dog>(5);

                // Assert
                var expected = new SqlCommand(@"
DELETE FROM dog
WHERE id = @Id",
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
DELETE FROM CompositeKeys
WHERE key1 = @Key1 AND key2 = @Key2",
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
DELETE FROM KeyNotGenerated
WHERE id = @Id",
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
DELETE FROM KeyAlias
WHERE Key = @Id",
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
DELETE FROM KeyExplicit
WHERE key = @Key",
                    new Dictionary<string, object>
                    {
                        ["Key"] = 5
                    });

                command.Should().Be(expected);
            }
        }

        public class MakeDeleteRangeStatement
            : PostgreSqlDialectTests
        {
            [Fact]
            public void Deletes_from_given_table()
            {
                // Act
                var command = this.Sut.MakeDeleteRangeCommand<Dog>("WHERE [Age] > @Age", new { Age = 10 });

                // Assert
                var expected = new SqlCommand(@"
DELETE FROM dog
WHERE [Age] > @Age",
                    new { Age = 10 });

                command.Should().Be(expected);
            }
        }

        public class MakeCreateTempTableStatement
            : PostgreSqlDialectTests
        {
            public MakeCreateTempTableStatement()
            {
                this.config = this.config.AddSqlTypeMapping(typeof(DateTime), DbType.DateTime2);
            }

            [Fact]
            public void Throws_exception_if_there_are_no_columns()
            {
                // Act
                Assert.Throws<ArgumentException>(() => this.Sut.MakeCreateTempTableCommand<NoColumns>());
            }

            [Fact]
            public void Creates_table_with_all_possible_types()
            {
                // Act
                var command = this.Sut.MakeCreateTempTableCommand<TempAllPossibleTypes>();

                // Assert
                var expected = new SqlCommand(@"CREATE TEMP TABLE TempAllPossibleTypes
(
    id INT NOT NULL,
    int16_property SMALLINT NOT NULL,
    nullable_int16_property SMALLINT NULL,
    int32_property INT NOT NULL,
    nullable_int32_property INT NULL,
    int64_property BIGINT NOT NULL,
    nullable_int64_property BIGINT NULL,
    single_property REAL NOT NULL,
    nullable_single_property REAL NULL,
    double_property DOUBLE PRECISION NOT NULL,
    nullable_double_property DOUBLE PRECISION NULL,
    decimal_property NUMERIC NOT NULL,
    nullable_decimal_property NUMERIC NULL,
    bool_property BOOL NOT NULL,
    nullable_bool_property BOOL NULL,
    string_property TEXT NOT NULL,
    nullable_string_property TEXT NULL,
    fixed_length_string_property TEXT NULL,
    char_property TEXT NOT NULL,
    nullable_char_property TEXT NULL,
    guid_property UUID NOT NULL,
    nullable_guid_property UUID NULL,
    date_time_property TIMESTAMP NOT NULL,
    nullable_date_time_property TIMESTAMP NULL,
    date_time_offset_property TIMESTAMP WITH TIME ZONE NOT NULL,
    nullable_date_time_offset_property TIMESTAMP WITH TIME ZONE NULL,
    byte_array_property BYTEA NOT NULL,
    color INT NOT NULL,
    nullable_color INT NULL
)");

                command.Should().Be(expected);
            }
        }

        public class MakeDropTempTableStatement
            : PostgreSqlDialectTests
        {
            [Fact]
            public void Drops_temporary_tables()
            {
                // Act
                var command = this.Sut.MakeDropTempTableCommand<Dog>();

                // Assert
                var expected = new SqlCommand(@"DROP TABLE dog");

                command.Should().Be(expected);
            }
        }
    }
}