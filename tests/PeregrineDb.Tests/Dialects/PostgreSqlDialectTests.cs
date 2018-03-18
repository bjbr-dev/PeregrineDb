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
        private PeregrineConfig config = DefaultConfig.MakeNewConfig().WithDialect(Dialect.PostgreSql);

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
FROM Users";

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
FROM Users
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
                var expected = @"SELECT Id, Name, Age
FROM Users
WHERE Id = @Id";

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
                var expected = @"SELECT Key, Name
FROM KeyExplicit
WHERE Key = @Key";

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
                var expected = @"SELECT Key1, Key2, Name
FROM CompositeKeys
WHERE Key1 = @Key1 AND Key2 = @Key2";

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
                var expected = @"SELECT Key AS Id, Name
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
                var expected = @"SELECT Id, YearsOld AS Age
FROM PropertyAlias
WHERE Id = @Id";

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
                var expected = @"SELECT Id, Name, Age
FROM Users";

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
                var expected = @"SELECT Id, Name, Age
FROM Users
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
                var expected = @"SELECT Key, Name
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
                var expected = @"SELECT Key AS Id, Name
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
                var expected = @"SELECT Id, YearsOld AS Age
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
                var expected = @"SELECT Id, Name, Age
FROM Users
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
                var expected = @"SELECT Id, Name, Age
FROM Users
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
                var expected = @"SELECT Id, YearsOld AS Age
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
                var expected = @"SELECT Id, Name, Age
FROM Users
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
                var expected = @"SELECT Id, Name, Age
FROM Users
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
                var expected = @"SELECT Id, Name, Age
FROM Users
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
                var expected = @"SELECT Id, YearsOld AS Age
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
                var expected = @"SELECT Id, Name, Age
FROM Users
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
                var expected = @"SELECT Id, Name, Age
FROM Users
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
                var expected = @"INSERT INTO Users (Name, Age)
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
                var expected = @"INSERT INTO KeyNotGenerated (Id, Name)
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
                var expected = @"INSERT INTO PropertyComputed (Name)
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
                var expected = @"INSERT INTO PropertyGenerated (Name)
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
                var expected = @"INSERT INTO Users (Name, Age)
VALUES (@Name, @Age)
RETURNING Id";

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
                var expected = @"INSERT INTO KeyNotGenerated (Id, Name)
VALUES (@Id, @Name)
RETURNING Id";

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
                var expected = @"INSERT INTO PropertyComputed (Name)
VALUES (@Name)
RETURNING Id";

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
                var expected = @"INSERT INTO PropertyGenerated (Name)
VALUES (@Name)
RETURNING Id";

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
                var expected = @"UPDATE Users
SET Name = @Name, Age = @Age
WHERE Id = @Id";

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
SET Name = @Name
WHERE Key1 = @Key1 AND Key2 = @Key2";

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
SET Name = @Name
WHERE Id = @Id";

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
WHERE Id = @Id";

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
SET Name = @Name
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
SET Name = @Name
WHERE Key = @Key";

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
SET Name = @Name
WHERE Id = @Id";

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
SET Name = @Name, Created = @Created
WHERE Id = @Id";

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
                var expected = @"DELETE FROM Users
WHERE Id = @Id";

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
WHERE Key1 = @Key1 AND Key2 = @Key2";

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
WHERE Id = @Id";

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
WHERE Key = @Key";

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
                var sql = this.config.Dialect.MakeDeleteRangeStatement(schema, "WHERE Age > 10");

                // Assert
                var expected = @"DELETE FROM Users
WHERE Age > 10";

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
                var expected = @"WHERE Name = @Name";

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
                var expected = @"WHERE Name = @Name AND Age = @Age";

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
                var expected = @"WHERE Name IS NULL";

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

                var defaultTableNameFactory = new DefaultTableNameFactory();
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
    Id INT NOT NULL,
    Int16Property SMALLINT NOT NULL,
    NullableInt16Property SMALLINT NULL,
    Int32Property INT NOT NULL,
    NullableInt32Property INT NULL,
    Int64Property BIGINT NOT NULL,
    NullableInt64Property BIGINT NULL,
    SingleProperty REAL NOT NULL,
    NullableSingleProperty REAL NULL,
    DoubleProperty DOUBLE PRECISION NOT NULL,
    NullableDoubleProperty DOUBLE PRECISION NULL,
    DecimalProperty NUMERIC NOT NULL,
    NullableDecimalProperty NUMERIC NULL,
    BoolProperty BOOL NOT NULL,
    NullableBoolProperty BOOL NULL,
    StringProperty TEXT NOT NULL,
    NullableStringProperty TEXT NULL,
    FixedLengthStringProperty TEXT NULL,
    CharProperty TEXT NOT NULL,
    NullableCharProperty TEXT NULL,
    GuidProperty UUID NOT NULL,
    NullableGuidProperty UUID NULL,
    DateTimeProperty TIMESTAMP NOT NULL,
    NullableDateTimeProperty TIMESTAMP NULL,
    DateTimeOffsetProperty TIMESTAMP WITH TIME ZONE NOT NULL,
    NullableDateTimeOffsetProperty TIMESTAMP WITH TIME ZONE NULL,
    ByteArrayProperty BYTEA NOT NULL,
    Color INT NOT NULL,
    NullableColor INT NULL
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

                var defaultTableNameFactory = new DefaultTableNameFactory();
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