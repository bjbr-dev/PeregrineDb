namespace PeregrineDb.Tests.Testing
{
    using System;
    using System.Collections.Generic;
    using FluentAssertions;
    using PeregrineDb;
    using PeregrineDb.Dialects;
    using PeregrineDb.Testing;
    using PeregrineDb.Tests.ExampleEntities;
    using PeregrineDb.Tests.Utils;
    using Xunit;

    public abstract class DataWiperTests
    {
        public static IEnumerable<object[]> TestDialects => new[]
            {
                new[] { Dialect.SqlServer2012 },
                new[] { Dialect.PostgreSql }
            };
        
        public class DeleteAllData
            : DataWiperTests
        {
            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Deletes_data_from_simple_table(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    database.Insert(new Dog { Name = "Some Name 1", Age = 10 });
                    database.Insert(new Dog { Name = "Some Name 2", Age = 10 });
                    database.Insert(new Dog { Name = "Some Name 3", Age = 10 });
                    database.Insert(new Dog { Name = "Some Name 4", Age = 11 });

                    // Act
                    DataWiper.ClearAllData(database);

                    // Assert
                    database.Count<Dog>().Should().Be(0);
                }
            }

            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Ignores_specified_tables(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    database.Insert(new Dog { Name = "Some Name 1", Age = 10 });

                    string tableName;
                    switch (dialect)
                    {
                        case SqlServer2012Dialect _:
                            tableName = "dbo.Dogs";
                            break;
                        case PostgreSqlDialect _:
                            tableName = "public.dog";
                            break;
                        default:
                            throw new NotSupportedException("Unknown dialect: " + dialect.GetType().Name);
                    }

                    // Act
                    DataWiper.ClearAllData(database, new HashSet<string> { tableName });

                    // Assert
                    database.Count<Dog>().Should().Be(1);
                }
            }

            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Deletes_data_from_foreign_keyed_tables(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var dogId = database.Insert<int>(new Dog { Name = "Some Name 1", Age = 10 });

                    database.Insert(new SimpleForeignKey { Name = "Some Name 1", DogId = dogId });

                    // Act
                    DataWiper.ClearAllData(database);

                    // Assert
                    database.Count<Dog>().Should().Be(0);
                    database.Count<SimpleForeignKey>().Should().Be(0);
                }
            }

            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Errors_when_an_ignored_table_references_an_unignored_one(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var dogId = database.Insert<int>(new Dog { Name = "Some Name 1", Age = 10 });

                    database.Insert(new SimpleForeignKey { Name = "Some Name 1", DogId = dogId });

                    string tableName;
                    switch (dialect)
                    {
                        case SqlServer2012Dialect _:
                            tableName = "dbo.SimpleForeignKeys";
                            break;
                        case PostgreSqlDialect _:
                            tableName = "public.simple_foreign_key";
                            break;
                        default:
                            throw new NotSupportedException("Unknown dialect: " + dialect.GetType().Name);
                    }

                    // Act
                    Action act = () => DataWiper.ClearAllData(database, new HashSet<string> { tableName });

                    // Assert
                    act.Should().Throw<Exception>();

                    // Cleanup
                    database.DeleteAll<SimpleForeignKey>();
                }
            }

            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Deletes_data_from_self_referenced_foreign_keyed_tables(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
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
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
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
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
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
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    database.Insert(new SchemaOther { Name = "Other" });
                    string name;
                    switch (dialect)
                    {
                        case SqlServer2012Dialect _:
                            name = "Other.SchemaOther";
                            break;
                        case PostgreSqlDialect _:
                            name = "other.schemaother";
                            break;
                        default:
                            throw new NotSupportedException("Unknown dialect: " + dialect.GetType().Name);
                    }

                    // Act
                    DataWiper.ClearAllData(database, new HashSet<string> { name });

                    // Assert
                    database.Count<SchemaOther>().Should().Be(1);
                }
            }

            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Can_delete_tables_which_reference_another_twice(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var id = database.Insert<int>(new WipeMultipleForeignKeyTarget { Name = "Other" });
                    database.Insert(new WipeMultipleForeignKeySource { NameId = id, OptionalNameId = id });

                    // Act
                    DataWiper.ClearAllData(database);

                    // Assert
                    database.Count<WipeMultipleForeignKeyTarget>().Should().Be(0);
                }
            }
        }
    }
}