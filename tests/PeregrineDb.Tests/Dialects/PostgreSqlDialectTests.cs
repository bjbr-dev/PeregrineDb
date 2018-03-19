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
    public class PostgreSqlDialectTests
    {
        private PeregrineConfig config = DefaultPeregrineConfig.Postgres;

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
                var expected = @"SELECT COUNT(*)
FROM user";

                Assert.Equal(expected, sql, SqlStringComparer.Instance);
            }

            [Fact]
            public void Adds_conditions()
            {
                // Arrange
                var schema = this.config.User();

                // Act
                var sql = this.config.Dialect.MakeCountStatement(schema, "WHERE Foo IS NOT NULL");

                // Assert
                var expected = @"SELECT COUNT(*)
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
                var sql = this.config.Dialect.MakeFindStatement(schema);

                // Assert
                var expected = @"SELECT id, name, age
FROM user
WHERE id = @Id";

                Assert.Equal(expected, sql, SqlStringComparer.Instance);
            }

            [Fact]
            public void Uses_non_default_primary_key_name()
            {
                // Arrange
                var schema = this.config.KeyExplicit();

                // Act
                var sql = this.config.Dialect.MakeFindStatement(schema);

                // Assert
                var expected = @"SELECT key, name
FROM KeyExplicit
WHERE key = @Key";

                Assert.Equal(expected, sql, SqlStringComparer.Instance);
            }

            [Fact]
            public void Uses_each_key_in_composite_key()
            {
                // Arrange
                var schema = this.config.CompositeKeys();

                // Act
                var sql = this.config.Dialect.MakeFindStatement(schema);

                // Assert
                var expected = @"SELECT key1, key2, name
FROM CompositeKeys
WHERE key1 = @Key1 AND key2 = @Key2";

                Assert.Equal(expected, sql, SqlStringComparer.Instance);
            }

            [Fact]
            public void Adds_alias_when_primary_key_is_aliased()
            {
                // Arrange
                var schema = this.config.KeyAlias();

                // Act
                var sql = this.config.Dialect.MakeFindStatement(schema);

                // Assert
                var expected = @"SELECT Key AS Id, name
FROM KeyAlias
WHERE Key = @Id";

                Assert.Equal(expected, sql, SqlStringComparer.Instance);
            }

            [Fact]
            public void Adds_alias_when_column_name_is_aliased()
            {
                // Arrange
                var schema = this.config.PropertyAlias();

                // Act
                var sql = this.config.Dialect.MakeFindStatement(schema);

                // Assert
                var expected = @"SELECT id, YearsOld AS Age
FROM PropertyAlias
WHERE id = @Id";

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
                var expected = @"SELECT id, name, age
FROM user";

                Assert.Equal(expected, sql, SqlStringComparer.Instance);
            }

            [Fact]
            public void Adds_conditions_clause()
            {
                // Arrange
                var schema = this.config.User();

                // Act
                var sql = this.config.Dialect.MakeGetRangeStatement(schema, "WHERE Age > @Age");

                // Assert
                var expected = @"SELECT id, name, age
FROM user
WHERE Age > @Age";

                Assert.Equal(expected, sql, SqlStringComparer.Instance);
            }

            [Fact]
            public void Uses_non_default_primary_key_name()
            {
                // Arrange
                var schema = this.config.KeyExplicit();

                // Act
                var sql = this.config.Dialect.MakeGetRangeStatement(schema, null);

                // Assert
                var expected = @"SELECT key, name
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
                var expected = @"SELECT Key AS Id, name
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
                var expected = @"SELECT id, YearsOld AS Age
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
                var expected = @"SELECT id, name, age
FROM user
ORDER BY Name
LIMIT 1";

                Assert.Equal(expected, sql, SqlStringComparer.Instance);
            }

            [Fact]
            public void Adds_conditions_clause()
            {
                // Arrange
                var schema = this.config.User();

                // Act
                var sql = this.config.Dialect.MakeGetTopNStatement(schema, 1, "WHERE Name LIKE 'Foo%'", "Name");

                // Assert
                var expected = @"SELECT id, name, age
FROM user
WHERE Name LIKE 'Foo%'
ORDER BY Name
LIMIT 1";

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
                var expected = @"SELECT id, YearsOld AS Age
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
                var expected = @"SELECT id, name, age
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
                var expected = @"SELECT id, name, age
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
                var sql = this.config.Dialect.MakeGetPageStatement(schema, new Page(1, 10, true, 0, 9), "WHERE Name LIKE 'Foo%'", "Name");

                // Assert
                var expected = @"SELECT id, name, age
FROM user
WHERE Name LIKE 'Foo%'
ORDER BY Name
LIMIT 10 OFFSET 0";

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
                var expected = @"SELECT id, YearsOld AS Age
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
                var expected = @"SELECT id, name, age
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
                var expected = @"SELECT id, name, age
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
                var sql = this.config.Dialect.MakeInsertStatement(schema);

                // Assert
                var expected = @"INSERT INTO user (name, age)
VALUES (@Name, @Age);";

                Assert.Equal(expected, sql, SqlStringComparer.Instance);
            }

            [Fact]
            public void Adds_primary_key_if_its_not_generated_by_database()
            {
                // Arrange
                var schema = this.config.KeyNotGenerated();

                // Act
                var sql = this.config.Dialect.MakeInsertStatement(schema);

                // Assert
                var expected = @"INSERT INTO KeyNotGenerated (id, name)
VALUES (@Id, @Name);";

                Assert.Equal(expected, sql, SqlStringComparer.Instance);
            }

            [Fact]
            public void Does_not_include_computed_columns()
            {
                // Arrange
                var schema = this.config.PropertyComputed();

                // Act
                var sql = this.config.Dialect.MakeInsertStatement(schema);

                // Assert
                var expected = @"INSERT INTO PropertyComputed (name)
VALUES (@Name);";

                Assert.Equal(expected, sql, SqlStringComparer.Instance);
            }

            [Fact]
            public void Does_not_include_generated_columns()
            {
                // Arrange
                var schema = this.config.PropertyGenerated();

                // Act
                var sql = this.config.Dialect.MakeInsertStatement(schema);

                // Assert
                var expected = @"INSERT INTO PropertyGenerated (name)
VALUES (@Name);";

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
                var sql = this.config.Dialect.MakeInsertReturningIdentityStatement(schema);

                // Assert
                var expected = @"INSERT INTO user (name, age)
VALUES (@Name, @Age)
RETURNING id";

                Assert.Equal(expected, sql, SqlStringComparer.Instance);
            }

            [Fact]
            public void Adds_primary_key_if_its_not_generated_by_database()
            {
                // Arrange
                var schema = this.config.KeyNotGenerated();

                // Act
                var sql = this.config.Dialect.MakeInsertReturningIdentityStatement(schema);

                // Assert
                var expected = @"INSERT INTO KeyNotGenerated (id, name)
VALUES (@Id, @Name)
RETURNING id";

                Assert.Equal(expected, sql, SqlStringComparer.Instance);
            }

            [Fact]
            public void Does_not_include_computed_columns()
            {
                // Arrange
                var schema = this.config.PropertyComputed();

                // Act
                var sql = this.config.Dialect.MakeInsertReturningIdentityStatement(schema);

                // Assert
                var expected = @"INSERT INTO PropertyComputed (name)
VALUES (@Name)
RETURNING id";

                Assert.Equal(expected, sql, SqlStringComparer.Instance);
            }

            [Fact]
            public void Does_not_include_generated_columns()
            {
                // Arrange
                var schema = this.config.PropertyGenerated();

                // Act
                var sql = this.config.Dialect.MakeInsertReturningIdentityStatement(schema);

                // Assert
                var expected = @"INSERT INTO PropertyGenerated (name)
VALUES (@Name)
RETURNING id";

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
                var sql = this.config.Dialect.MakeUpdateStatement(schema);

                // Assert
                var expected = @"UPDATE user
SET name = @Name, age = @Age
WHERE id = @Id";

                Assert.Equal(expected, sql, SqlStringComparer.Instance);
            }

            [Fact]
            public void Uses_each_key_in_composite_key()
            {
                // Arrange
                var schema = this.config.CompositeKeys();

                // Act
                var sql = this.config.Dialect.MakeUpdateStatement(schema);

                // Assert
                var expected = @"UPDATE CompositeKeys
SET name = @Name
WHERE key1 = @Key1 AND key2 = @Key2";

                Assert.Equal(expected, sql, SqlStringComparer.Instance);
            }

            [Fact]
            public void Does_not_update_primary_key_even_if_its_not_auto_generated()
            {
                // Arrange
                var schema = this.config.KeyNotGenerated();

                // Act
                var sql = this.config.Dialect.MakeUpdateStatement(schema);

                // Assert
                var expected = @"UPDATE KeyNotGenerated
SET name = @Name
WHERE id = @Id";

                Assert.Equal(expected, sql, SqlStringComparer.Instance);
            }

            [Fact]
            public void Uses_aliased_property_names()
            {
                // Arrange
                var schema = this.config.PropertyAlias();

                // Act
                var sql = this.config.Dialect.MakeUpdateStatement(schema);

                // Assert
                var expected = @"UPDATE PropertyAlias
SET YearsOld = @Age
WHERE id = @Id";

                Assert.Equal(expected, sql, SqlStringComparer.Instance);
            }

            [Fact]
            public void Uses_aliased_key_name()
            {
                // Arrange
                var schema = this.config.KeyAlias();

                // Act
                var sql = this.config.Dialect.MakeUpdateStatement(schema);

                // Assert
                var expected = @"UPDATE KeyAlias
SET name = @Name
WHERE Key = @Id";

                Assert.Equal(expected, sql, SqlStringComparer.Instance);
            }

            [Fact]
            public void Uses_explicit_key_name()
            {
                // Arrange
                var schema = this.config.KeyExplicit();

                // Act
                var sql = this.config.Dialect.MakeUpdateStatement(schema);

                // Assert
                var expected = @"UPDATE KeyExplicit
SET name = @Name
WHERE key = @Key";

                Assert.Equal(expected, sql, SqlStringComparer.Instance);
            }

            [Fact]
            public void Does_not_include_computed_columns()
            {
                // Arrange
                var schema = this.config.PropertyComputed();

                // Act
                var sql = this.config.Dialect.MakeUpdateStatement(schema);

                // Assert
                var expected = @"UPDATE PropertyComputed
SET name = @Name
WHERE id = @Id";

                Assert.Equal(expected, sql, SqlStringComparer.Instance);
            }

            [Fact]
            public void Includes_generated_columns()
            {
                // Arrange
                var schema = this.config.PropertyGenerated();

                // Act
                var sql = this.config.Dialect.MakeUpdateStatement(schema);

                // Assert
                var expected = @"UPDATE PropertyGenerated
SET name = @Name, created = @Created
WHERE id = @Id";

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
                var sql = this.config.Dialect.MakeDeleteByPrimaryKeyStatement(schema);

                // Assert
                var expected = @"DELETE FROM user
WHERE id = @Id";

                Assert.Equal(expected, sql, SqlStringComparer.Instance);
            }

            [Fact]
            public void Uses_each_key_in_composite_key()
            {
                // Arrange
                var schema = this.config.CompositeKeys();

                // Act
                var sql = this.config.Dialect.MakeDeleteByPrimaryKeyStatement(schema);

                // Assert
                var expected = @"DELETE FROM CompositeKeys
WHERE key1 = @Key1 AND key2 = @Key2";

                Assert.Equal(expected, sql, SqlStringComparer.Instance);
            }

            [Fact]
            public void Uses_primary_key_even_if_its_not_auto_generated()
            {
                // Arrange
                var schema = this.config.KeyNotGenerated();

                // Act
                var sql = this.config.Dialect.MakeDeleteByPrimaryKeyStatement(schema);

                // Assert
                var expected = @"DELETE FROM KeyNotGenerated
WHERE id = @Id";

                Assert.Equal(expected, sql, SqlStringComparer.Instance);
            }

            [Fact]
            public void Uses_aliased_key_name()
            {
                // Arrange
                var schema = this.config.KeyAlias();

                // Act
                var sql = this.config.Dialect.MakeDeleteByPrimaryKeyStatement(schema);

                // Assert
                var expected = @"DELETE FROM KeyAlias
WHERE Key = @Id";

                Assert.Equal(expected, sql, SqlStringComparer.Instance);
            }

            [Fact]
            public void Uses_explicit_key_name()
            {
                // Arrange
                var schema = this.config.KeyExplicit();

                // Act
                var sql = this.config.Dialect.MakeDeleteByPrimaryKeyStatement(schema);

                // Assert
                var expected = @"DELETE FROM KeyExplicit
WHERE key = @Key";

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
                var sql = this.config.Dialect.MakeDeleteRangeStatement(schema, "WHERE age > 10");

                // Assert
                var expected = @"DELETE FROM user
WHERE age > 10";

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
                var expected = string.Empty;
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
                var expected = @"WHERE name = @Name";

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
                var expected = @"WHERE name = @Name AND age = @Age";

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
                var expected = @"WHERE name IS NULL";

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
            private readonly Mock<ITableNameFactory> tableNameFactory;

            public MakeCreateTempTableStatement()
            {
                this.config = this.config.AddSqlTypeMapping(typeof(DateTime), DbType.DateTime2);
                this.tableNameFactory = new Mock<ITableNameFactory>();

                var defaultTableNameFactory = new AtttributeTableNameFactory();
                this.tableNameFactory.Setup(f => f.GetTableName(It.IsAny<Type>(), It.IsAny<IDialect>()))
                    .Returns((Type type, IDialect d) => defaultTableNameFactory.GetTableName(type, d));
                this.config = this.config.WithTableNameFactory(this.tableNameFactory.Object);
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
                var expected = @"CREATE TEMP TABLE TempAllPossibleTypes
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
            private readonly Mock<ITableNameFactory> tableNameFactory;

            public MakeDropTempTableStatement()
            {
                this.tableNameFactory = new Mock<ITableNameFactory>();

                var defaultTableNameFactory = new AtttributeTableNameFactory();
                this.tableNameFactory.Setup(f => f.GetTableName(It.IsAny<Type>(), It.IsAny<IDialect>()))
                    .Returns((Type type, IDialect d) => defaultTableNameFactory.GetTableName(type, d));

                this.config = this.config.AddSqlTypeMapping(typeof(DateTime), DbType.DateTime2)
                                  .WithTableNameFactory(this.tableNameFactory.Object);
            }

            [Fact]
            public void Drops_temporary_tables()
            {
                // Arrange
                var tableSchema = this.config.MakeSchema<User>();

                // Act
                var sql = this.config.Dialect.MakeDropTempTableStatement(tableSchema);

                // Assert
                var expected = @"DROP TABLE Users";

                Assert.Equal(expected, sql, SqlStringComparer.Instance);
            }
        }
    }
}