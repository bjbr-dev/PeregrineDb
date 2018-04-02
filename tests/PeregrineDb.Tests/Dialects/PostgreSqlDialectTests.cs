namespace PeregrineDb.Tests.Dialects
{
    using System;
    using System.Collections.Immutable;
    using System.Data;
    using System.Diagnostics.CodeAnalysis;
    using Pagination;
    using PeregrineDb;
    using PeregrineDb.Schema;
    using PeregrineDb.Tests.ExampleEntities;
    using PeregrineDb.Tests.Utils;
    using Xunit;

    [SuppressMessage("ReSharper", "ConvertToConstant.Local")]
    public class PostgreSqlDialectTests
    {
        private PeregrineConfig config = PeregrineConfig.Postgres;

        public class MakeCountStatement
            : PostgreSqlDialectTests
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
FROM user";

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
FROM user
WHERE Foo IS NOT NULL";

                Assert.Equal(expected, sql, SqlStringComparer.Instance);
            }
        }

        public class MakeFindStatement
            : PostgreSqlDialectTests
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
SELECT id, name, age
FROM user
WHERE id = {0}", 5, null, null);

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
SELECT key, name
FROM KeyExplicit
WHERE key = {0}", 5, null);

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
SELECT key1, key2, name
FROM CompositeKeys
WHERE key1 = {0} AND key2 = {1}", 2, 3, null);

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
SELECT Key AS Id, name
FROM KeyAlias
WHERE Key = {0}", 5, null);

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
SELECT id, YearsOld AS Age
FROM PropertyAlias
WHERE id = {0}", 5, null);

                Assert.Equal(expected, sql, SqlStringComparer.Instance);
            }
        }

        public class MakeGetRangeStatement
            : PostgreSqlDialectTests
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
SELECT id, name, age
FROM user";

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
SELECT id, name, age
FROM user
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
SELECT key, name
FROM KeyExplicit";

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
SELECT Key AS Id, name
FROM KeyAlias";

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
SELECT id, YearsOld AS Age
FROM PropertyAlias";

                Assert.Equal(expected, sql, SqlStringComparer.Instance);
            }
        }

        public class MakeGetTopNStatement
            : PostgreSqlDialectTests
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
SELECT id, name, age
FROM user
ORDER BY Name
LIMIT 1");

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
SELECT id, name, age
FROM user
WHERE Name LIKE {0}
ORDER BY Name
LIMIT 1", "Foo%");

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
SELECT id, YearsOld AS Age
FROM PropertyAlias
ORDER BY Name
LIMIT 1";

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
SELECT id, name, age
FROM user
LIMIT 1";

                Assert.Equal(expected, sql, SqlStringComparer.Instance);
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
SELECT id, name, age
FROM user
ORDER BY Name
LIMIT 10 OFFSET 0";

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
SELECT id, name, age
FROM user
WHERE Name LIKE {0}
ORDER BY Name
LIMIT 10 OFFSET 0", "Foo%");

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
SELECT id, YearsOld AS Age
FROM PropertyAlias
ORDER BY Name
LIMIT 10 OFFSET 0";

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
SELECT id, name, age
FROM user
ORDER BY Name
LIMIT 10 OFFSET 10";

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
SELECT id, name, age
FROM user
ORDER BY Name
LIMIT 5 OFFSET 5";

                Assert.Equal(expected, sql, SqlStringComparer.Instance);
            }
        }

        public class MakeInsertStatement
            : PostgreSqlDialectTests
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
INSERT INTO user (name, age)
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
INSERT INTO KeyNotGenerated (id, name)
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
INSERT INTO PropertyComputed (name)
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
INSERT INTO PropertyGenerated (name)
VALUES ({1});", 0, "Foo", default(DateTime));

                Assert.Equal(expected, sql, SqlStringComparer.Instance);
            }
        }

        public class MakeInsertReturningIdentityStatement
            : PostgreSqlDialectTests
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
INSERT INTO user (name, age)
VALUES ({1}, {2})
RETURNING id", 0, "Foo", 10);

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
INSERT INTO KeyNotGenerated (id, name)
VALUES ({0}, {1})
RETURNING id", 10, "Foo");

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
INSERT INTO PropertyComputed (name)
VALUES ({1})
RETURNING id", 0, "Foo", default(DateTime));

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
INSERT INTO PropertyGenerated (name)
VALUES ({1})
RETURNING id", 0, "Foo", default(DateTime));

                Assert.Equal(expected, sql, SqlStringComparer.Instance);
            }
        }

        public class MakeUpdateStatement
            : PostgreSqlDialectTests
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
UPDATE user
SET name = {1}, age = {2}
WHERE id = {0}", 5, "Foo", 10);

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
UPDATE CompositeKeys
SET name = {2}
WHERE key1 = {0} AND key2 = {1}", 7, 8, "Foo");

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
UPDATE KeyNotGenerated
SET name = {1}
WHERE id = {0}", 7, "Foo");

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
UPDATE PropertyAlias
SET YearsOld = {1}
WHERE id = {0}", 5, 10);

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
UPDATE KeyAlias
SET name = {1}
WHERE Key = {0}", 10, "Foo");

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
UPDATE KeyExplicit
SET name = {1}
WHERE key = {0}", 10, "Foo");

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
UPDATE PropertyComputed
SET name = {1}
WHERE id = {0}", 10, "Foo", default(DateTime));

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
UPDATE PropertyGenerated
SET name = {1}, created = {2}
WHERE id = {0}", 5, "Foo", new DateTime(2018, 4, 1));

                Assert.Equal(expected, sql, SqlStringComparer.Instance);
            }
        }

        public class MakeDeleteByPrimaryKeyStatement
            : PostgreSqlDialectTests
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
DELETE FROM user
WHERE id = {0}", 5, null, null);

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
DELETE FROM CompositeKeys
WHERE key1 = {0} AND key2 = {1}", 1, 2, null as string);

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
DELETE FROM KeyNotGenerated
WHERE id = {0}", 5, null);

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
DELETE FROM KeyAlias
WHERE Key = {0}", 5, null);

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
DELETE FROM KeyExplicit
WHERE key = {0}", 5, null);

                Assert.Equal(expected, sql, SqlStringComparer.Instance);
            }
        }

        public class MakeDeleteRangeStatement
            : PostgreSqlDialectTests
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
DELETE FROM user
WHERE [Age] > {0}", 10);

                Assert.Equal(expected, sql, SqlStringComparer.Instance);
            }
        }

        public class MakeWhereClause
            : PostgreSqlDialectTests
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
                var expected = new SqlString("WHERE name = {1}", null, "Bobby");

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
                FormattableString expected = new SqlString(@"WHERE name = {1} AND age = {2}", null, "Bobby", 5);

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
                var expected = new SqlString("WHERE name IS NULL", null, null);

                Assert.Equal(expected, sql, SqlStringComparer.Instance);
            }

            private ImmutableArray<ConditionColumnSchema> GetConditionsSchema<TEntity>(object value)
            {
                var tableSchema = this.config.GetTableSchema(typeof(TEntity));
                return this.config.GetConditionsSchema(typeof(TEntity), tableSchema, value.GetType());
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
                Assert.Throws<ArgumentException>(() => this.config.Dialect.MakeCreateTempTableStatement(this.config.NoColumns()));
            }

            [Fact]
            public void Creates_table_with_all_possible_types()
            {
                // Act
                var sql = this.config.Dialect.MakeCreateTempTableStatement(this.config.TempAllPossibleTypes());

                // Assert
                FormattableString expected = $@"CREATE TEMP TABLE TempAllPossibleTypes
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
)";

                Assert.Equal(expected, sql, SqlStringComparer.Instance);
            }
        }

        public class MakeDropTempTableStatement
            : PostgreSqlDialectTests
        {
            [Fact]
            public void Drops_temporary_tables()
            {
                // Arrange
                var tableSchema = this.config.MakeSchema<User>();

                // Act
                var sql = this.config.Dialect.MakeDropTempTableStatement(tableSchema);

                // Assert
                FormattableString expected = $@"DROP TABLE user";

                Assert.Equal(expected, sql, SqlStringComparer.Instance);
            }
        }
    }
}