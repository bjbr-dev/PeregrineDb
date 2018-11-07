namespace PeregrineDb.Tests.Dialects
{
    using System;
    using System.Collections.Generic;
    using System.Collections.Immutable;
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
            return this.GetTableSchema(typeof(T));
        }

        private TableSchema GetTableSchema(Type type)
        {
            return this.config.SchemaFactory.GetTableSchema(type);
        }

        private ImmutableArray<ConditionColumnSchema> GetConditionsSchema<T>(object conditions)
        {
            var entityType = typeof(T);
            var tableSchema = this.GetTableSchema(entityType);
            return this.config.SchemaFactory.GetConditionsSchema(entityType, tableSchema, conditions.GetType());
        }

        public class MakeCountStatement
            : PostgreSqlDialectTests
        {
            [Fact]
            public void Selects_from_given_table()
            {
                // Act
                var command = this.Sut.MakeCountCommand(null, null, this.GetTableSchema<Dog>());

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
                var command = this.Sut.MakeCountCommand("WHERE Foo IS NOT NULL", null, this.GetTableSchema<Dog>());

                // Assert
                var expected = new SqlCommand(@"
SELECT COUNT(*)
FROM dog
WHERE Foo IS NOT NULL");

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
                Action act = () => this.Sut.MakeFindCommand(null, this.GetTableSchema<Dog>());

                // Assert
                act.Should().Throw<ArgumentNullException>();
            }

            [Fact]
            public void Selects_from_given_table()
            {
                // Act
                var command = this.Sut.MakeFindCommand(5, this.GetTableSchema<Dog>());

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
                var command = this.Sut.MakeFindCommand(5, this.GetTableSchema<KeyExplicit>());

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
                var command = this.Sut.MakeFindCommand(new { key1 = 2, key2 = 3 }, this.GetTableSchema<CompositeKeys>());

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
                var command = this.Sut.MakeFindCommand(5, this.GetTableSchema<KeyAlias>());

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
                var command = this.Sut.MakeFindCommand(5, this.GetTableSchema<PropertyAlias>());

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

        public class MakeGetRangeStatement
            : PostgreSqlDialectTests
        {
            [Fact]
            public void Selects_from_given_table()
            {
                // Act
                var command = this.Sut.MakeGetRangeCommand(null, null, this.GetTableSchema<Dog>());

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
                var command = this.Sut.MakeGetRangeCommand("WHERE Age > @Age", new { Age = 10 }, this.GetTableSchema<Dog>());

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
                var command = this.Sut.MakeGetRangeCommand(null, null, this.GetTableSchema<KeyExplicit>());

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
                var command = this.Sut.MakeGetRangeCommand(null, null, this.GetTableSchema<KeyAlias>());

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
                var command = this.Sut.MakeGetRangeCommand(null, null, this.GetTableSchema<PropertyAlias>());

                // Assert
                var expected = new SqlCommand(@"
SELECT id, YearsOld AS Age
FROM PropertyAlias");

                command.Should().Be(expected);
            }
        }

        public class MakeGetFirstNCommand
            : PostgreSqlDialectTests
        {
            [Fact]
            public void Adds_conditions_clause()
            {
                // Act
                var command = this.Sut.MakeGetFirstNCommand(1, "WHERE Name LIKE @Name", new { Name = "Foo%" }, "Name", this.GetTableSchema<Dog>());

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
                var command = this.Sut.MakeGetFirstNCommand(1, "WHERE Name LIKE @Name", new { Name = "Foo%" }, "Name", this.GetTableSchema<PropertyAlias>());

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
                var command = this.Sut.MakeGetFirstNCommand(1, "WHERE Name LIKE @Name", new { Name = "Foo%" }, orderBy, this.GetTableSchema<Dog>());

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
                Assert.Throws<ArgumentException>(() => this.Sut.MakeGetPageCommand(new Page(1, 10, true, 0, 9), null, null, orderBy, this.GetTableSchema<Dog>()));
            }

            [Fact]
            public void Selects_from_given_table()
            {
                // Act
                var command = this.Sut.MakeGetPageCommand(new Page(1, 10, true, 0, 9), null, null, "Name", this.GetTableSchema<Dog>());

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
                var command = this.Sut.MakeGetPageCommand(new Page(1, 10, true, 0, 9), "WHERE Name LIKE @Name", new { Name = "Foo%" }, "Name", this.GetTableSchema<Dog>());

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
                var command = this.Sut.MakeGetPageCommand(new Page(1, 10, true, 0, 9), null, null, "Name", this.GetTableSchema<PropertyAlias>());

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
                var command = this.Sut.MakeGetPageCommand(new Page(2, 10, true, 10, 19), null, null, "Name", this.GetTableSchema<Dog>());

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
                var command = this.Sut.MakeGetPageCommand(new Page(2, 5, true, 5, 9), null, null, "Name", this.GetTableSchema<Dog>());

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
                object entity = new Dog { Name = "Foo", Age = 10 };
                var command = this.Sut.MakeInsertCommand(entity, this.GetTableSchema<Dog>());

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
                object entity = new KeyNotGenerated { Id = 6, Name = "Foo" };
                var command = this.Sut.MakeInsertCommand(entity, this.GetTableSchema<KeyNotGenerated>());

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
                object entity = new PropertyComputed { Name = "Foo" };
                var command = this.Sut.MakeInsertCommand(entity, this.GetTableSchema<PropertyComputed>());

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
                object entity = new PropertyGenerated { Name = "Foo" };
                var command = this.Sut.MakeInsertCommand(entity, this.GetTableSchema<PropertyGenerated>());

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
                var command = this.Sut.MakeInsertReturningPrimaryKeyCommand(new Dog { Name = "Foo", Age = 10 }, this.GetTableSchema<Dog>());

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
                var command = this.Sut.MakeInsertReturningPrimaryKeyCommand(new KeyNotGenerated { Id = 10, Name = "Foo" }, this.GetTableSchema<KeyNotGenerated>());

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
                var command = this.Sut.MakeInsertReturningPrimaryKeyCommand(new PropertyComputed { Name = "Foo" }, this.GetTableSchema<PropertyComputed>());

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
                var command = this.Sut.MakeInsertReturningPrimaryKeyCommand(new PropertyGenerated { Name = "Foo" }, this.GetTableSchema<PropertyGenerated>());

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
                var command = this.Sut.MakeUpdateCommand(new Dog { Id = 5, Name = "Foo", Age = 10 }, this.GetTableSchema<Dog>());

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
                var command = this.Sut.MakeUpdateCommand(new CompositeKeys { Key1 = 7, Key2 = 8, Name = "Foo" }, this.GetTableSchema<CompositeKeys>());

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
                var command = this.Sut.MakeUpdateCommand(new KeyNotGenerated { Id = 7, Name = "Foo" }, this.GetTableSchema<KeyNotGenerated>());

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
                var command = this.Sut.MakeUpdateCommand(new PropertyAlias { Id = 5, Age = 10 }, this.GetTableSchema<PropertyAlias>());

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
                var command = this.Sut.MakeUpdateCommand(new KeyAlias { Name = "Foo", Id = 10 }, this.GetTableSchema<KeyAlias>());

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
                var command = this.Sut.MakeUpdateCommand(new KeyExplicit { Name = "Foo", Key = 10 }, this.GetTableSchema<KeyExplicit>());

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
                var command = this.Sut.MakeUpdateCommand(new PropertyComputed { Name = "Foo", Id = 10 }, this.GetTableSchema<PropertyComputed>());

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
                var command = this.Sut.MakeUpdateCommand(new PropertyGenerated { Id = 5, Name = "Foo", Created = new DateTime(2018, 4, 1) }, this.GetTableSchema<PropertyGenerated>());

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
                var command = this.Sut.MakeDeleteByPrimaryKeyCommand(5, this.GetTableSchema<Dog>());

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
                var command = this.Sut.MakeDeleteByPrimaryKeyCommand(new { Key1 = 1, Key2 = 2 }, this.GetTableSchema<CompositeKeys>());

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
                var command = this.Sut.MakeDeleteByPrimaryKeyCommand(5, this.GetTableSchema<KeyNotGenerated>());

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
                var command = this.Sut.MakeDeleteByPrimaryKeyCommand(5, this.GetTableSchema<KeyAlias>());

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
                var command = this.Sut.MakeDeleteByPrimaryKeyCommand(5, this.GetTableSchema<KeyExplicit>());

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
                var command = this.Sut.MakeDeleteRangeCommand("WHERE [Age] > @Age", new { Age = 10 }, this.GetTableSchema<Dog>());

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
                Assert.Throws<ArgumentException>(() => this.Sut.MakeCreateTempTableCommand(this.GetTableSchema<NoColumns>()));
            }

            [Fact]
            public void Creates_table_with_all_possible_types()
            {
                // Act
                var command = this.Sut.MakeCreateTempTableCommand(this.GetTableSchema<TempAllPossibleTypes>());

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
                var command = this.Sut.MakeDropTempTableCommand(this.GetTableSchema<Dog>());

                // Assert
                var expected = new SqlCommand(@"DROP TABLE dog");

                command.Should().Be(expected);
            }
        }

        public class MakeWhereClause
            : PostgreSqlDialectTests
        {
            [Fact]
            public void Returns_empty_string_when_conditions_is_empty()
            {
                // Arrange
                var conditions = new { };
                var conditionsSchema = this.GetConditionsSchema<Dog>(conditions);

                // Act
                var clause = this.Sut.MakeWhereClause(conditionsSchema, conditions);

                // Assert
                clause.Should().BeEmpty();
            }

            [Fact]
            public void Selects_from_given_table()
            {
                // Arrange
                var conditions = new { Name = "Fido" };
                var conditionsSchema = this.GetConditionsSchema<Dog>(conditions);

                // Act
                var clause = this.Sut.MakeWhereClause(conditionsSchema, conditions);

                // Assert
                clause.Should().Be("WHERE name = @Name");
            }

            [Fact]
            public void Adds_alias_when_column_name_is_aliased()
            {
                // Arrange
                var conditions = new { Age = 15 };
                var conditionsSchema = this.GetConditionsSchema<PropertyAlias>(conditions);

                // Act
                var clause = this.Sut.MakeWhereClause(conditionsSchema, conditions);

                // Assert
                clause.Should().Be("WHERE YearsOld = @Age");
            }

            [Fact]
            public void Checks_multiple_properties()
            {
                // Arrange
                var conditions = new { Name = "Fido", Age = 15 };
                var conditionsSchema = this.GetConditionsSchema<Dog>(conditions);

                // Act
                var clause = this.Sut.MakeWhereClause(conditionsSchema, conditions);

                // Assert
                clause.Should().Be("WHERE name = @Name AND age = @Age");
            }

            [Fact]
            public void Checks_for_null_properly()
            {
                // Arrange
                var conditions = new { Name = (string)null };
                var conditionsSchema = this.GetConditionsSchema<Dog>(conditions);

                // Act
                var clause = this.Sut.MakeWhereClause(conditionsSchema, conditions);

                // Assert
                clause.Should().Be("WHERE name IS NULL");
            }

            [Fact]
            public void Checks_for_null_properly_with_multiple_properties()
            {
                // Arrange
                var conditions = new { Name = (string)null, age = (int?)null };
                var conditionsSchema = this.GetConditionsSchema<Dog>(conditions);

                // Act
                var clause = this.Sut.MakeWhereClause(conditionsSchema, conditions);

                // Assert
                clause.Should().Be("WHERE name IS NULL AND age IS NULL");
            }
        }
    }
}