namespace PeregrineDb.Tests
{
    using System;
    using System.Collections.Generic;
    using FluentAssertions;
    using PeregrineDb;
    using PeregrineDb.Dialects;
    using PeregrineDb.Tests.ExampleEntities;
    using PeregrineDb.Tests.Utils;
    using Xunit;

    public abstract class DataWiperTests
    {
        private static IEnumerable<IDialect> TestDialects => new[]
            {
                Dialect.SqlServer2012,
                Dialect.PostgreSql
            };
        
        public abstract class DeleteAllData
            : DataWiperTests
        {
            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Deletes_data_from_simple_table(IDialect dialect)
            {
                using (var instance = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var database = instance.Item;

                    database.Insert(new User { Name = "Some Name 1", Age = 10 });
                    database.Insert(new User { Name = "Some Name 2", Age = 10 });
                    database.Insert(new User { Name = "Some Name 3", Age = 10 });
                    database.Insert(new User { Name = "Some Name 4", Age = 11 });

                    // Act
                    DataWiper.ClearAllData(database);

                    // Assert
                    database.Count<User>().Should().Be(0);
                }
            }

            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Ignores_specified_tables(IDialect dialect)
            {
                using (var instance = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var database = instance.Item;
                    database.Insert(new User { Name = "Some Name 1", Age = 10 });

                    // Act
                    DataWiper.ClearAllData(database, new HashSet<string> { "dbo.Users" });

                    // Assert
                    database.Count<User>().Should().Be(1);
                }
            }

            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Deletes_data_from_foreign_keyed_tables(IDialect dialect)
            {
                using (var instance = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var database = instance.Item;
                    var userId = database.Insert<int>(new User { Name = "Some Name 1", Age = 10 });

                    database.Insert(new SimpleForeignKey { Name = "Some Name 1", UserId = userId });

                    // Act
                    DataWiper.ClearAllData(database);

                    // Assert
                    database.Count<User>().Should().Be(0);
                    database.Count<SimpleForeignKey>().Should().Be(0);
                }
            }

            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Errors_when_an_ignored_table_references_an_unignored_one(IDialect dialect)
            {
                using (var instance = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var database = instance.Item;
                    var userId = database.Insert<int>(new User { Name = "Some Name 1", Age = 10 });

                    database.Insert(new SimpleForeignKey { Name = "Some Name 1", UserId = userId });

                    // Act
                    Action act = () => DataWiper.ClearAllData(database, new HashSet<string> { "dbo.SimpleForeignKeys" });

                    // Assert
                    act.ShouldThrow<Exception>();

                    // Cleanup
                    database.DeleteAll<SimpleForeignKey>();
                }
            }

            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Deletes_data_from_self_referenced_foreign_keyed_tables(IDialect dialect)
            {
                using (var instance = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var database = instance.Item;
                    var id = database.Insert<int>(new SelfReferenceForeignKey());
                    database.Insert(new SelfReferenceForeignKey { ForeignId = id });

                    // Act
                    DataWiper.ClearAllData(database);

                    // Assert
                    database.Count<SelfReferenceForeignKey>().Should().Be(0);
                }
            }

            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Deletes_data_from_cyclic_foreign_keyed_tables(IDialect dialect)
            {
                using (var instance = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var database = instance.Item;
                    var a = new CyclicForeignKeyA();
                    a.Id = database.Insert<int>(a);

                    var b = new CyclicForeignKeyB { ForeignId = a.Id };
                    b.Id = database.Insert<int>(b);

                    var c = new CyclicForeignKeyC { ForeignId = b.Id };
                    c.Id = database.Insert<int>(c);

                    a.ForeignId = c.Id;
                    database.Update(a);

                    // Act
                    DataWiper.ClearAllData(database);

                    // Assert
                    database.Count<CyclicForeignKeyA>().Should().Be(0);
                    database.Count<CyclicForeignKeyB>().Should().Be(0);
                    database.Count<CyclicForeignKeyC>().Should().Be(0);
                }
            }

            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Deletes_data_from_tables_in_other_schemas(IDialect dialect)
            {
                using (var instance = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var database = instance.Item;
                    var otherId = database.Insert<int>(new SchemaOther { Name = "Other" });
                    database.Insert(new SchemaSimpleForeignKeys { SchemaOtherId = otherId });

                    // Act
                    DataWiper.ClearAllData(database);

                    // Assert
                    database.Count<SchemaOther>().Should().Be(0);
                    database.Count<SchemaSimpleForeignKeys>().Should().Be(0);
                }
            }

            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Ignores_tables_in_other_schemas(IDialect dialect)
            {
                using (var instance = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var database = instance.Item;
                    database.Insert(new SchemaOther { Name = "Other" });

                    // Act
                    DataWiper.ClearAllData(database, new HashSet<string> { "Other.SchemaOther" });

                    // Assert
                    database.Count<SchemaOther>().Should().Be(1);
                }
            }
        }
    }
}