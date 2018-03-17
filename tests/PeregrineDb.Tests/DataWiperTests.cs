namespace Dapper.MicroCRUD.Tests
{
    using System;
    using Dapper.MicroCRUD.Databases;
    using Dapper.MicroCRUD.Tests.Dialects.SqlServer;
    using Dapper.MicroCRUD.Tests.ExampleEntities;
    using FluentAssertions;
    using Xunit;

    public abstract class DataWiperTests
    {
        private readonly IDatabase database;

        protected DataWiperTests(DatabaseFixture fixture)
        {
            this.database = fixture?.DefaultDatabase;
        }

        public abstract class DeleteAllData
            : DataWiperTests
        {
            protected DeleteAllData(DatabaseFixture fixture)
                : base(fixture)
            {
            }

            [Fact]
            public void Deletes_data_from_simple_table()
            {
                // Arrange
                this.database.Insert(new User { Name = "Some Name 1", Age = 10 });
                this.database.Insert(new User { Name = "Some Name 2", Age = 10 });
                this.database.Insert(new User { Name = "Some Name 3", Age = 10 });
                this.database.Insert(new User { Name = "Some Name 4", Age = 11 });

                var sut = new DataWiper();

                // Act
                sut.ClearAllData(this.database);

                // Assert
                this.database.Count<User>().Should().Be(0);
            }

            [Fact]
            public void Ignores_specified_tables()
            {
                // Arrange
                this.database.Insert(new User { Name = "Some Name 1", Age = 10 });

                var sut = new DataWiper
                    {
                        IgnoredTables = new[]
                            {
                                "dbo.Users"
                            }
                    };

                // Act
                sut.ClearAllData(this.database);

                // Assert
                this.database.Count<User>().Should().Be(1);
            }

            [Fact]
            public void Deletes_data_from_foreign_keyed_tables()
            {
                // Arrange
                var userId = this.database.Insert<int>(new User { Name = "Some Name 1", Age = 10 });

                this.database.Insert(new SimpleForeignKey { Name = "Some Name 1", UserId = userId });

                var sut = new DataWiper();

                // Act
                sut.ClearAllData(this.database);

                // Assert
                this.database.Count<User>().Should().Be(0);
                this.database.Count<SimpleForeignKey>().Should().Be(0);
            }

            [Fact]
            public void Errors_when_an_ignored_table_references_an_unignored_one()
            {
                // Arrange
                var userId = this.database.Insert<int>(new User { Name = "Some Name 1", Age = 10 });

                this.database.Insert(new SimpleForeignKey { Name = "Some Name 1", UserId = userId });

                var sut = new DataWiper
                    {
                        IgnoredTables = new[] { "dbo.SimpleForeignKeys" }
                    };

                // Act
                Action act = () => sut.ClearAllData(this.database);

                // Assert
                act.ShouldThrow<Exception>();

                // Cleanup
                this.database.DeleteAll<SimpleForeignKey>();
            }

            [Fact]
            public void Deletes_data_from_self_referenced_foreign_keyed_tables()
            {
                // Arrange
                var id = this.database.Insert<int>(new SelfReferenceForeignKey());
                this.database.Insert(new SelfReferenceForeignKey { ForeignId = id });

                var sut = new DataWiper();

                // Act
                sut.ClearAllData(this.database);

                // Assert
                this.database.Count<SelfReferenceForeignKey>().Should().Be(0);
            }

            [Fact]
            public void Deletes_data_from_cyclic_foreign_keyed_tables()
            {
                // Arrange
                var a = new CyclicForeignKeyA();
                a.Id = this.database.Insert<int>(a);

                var b = new CyclicForeignKeyB { ForeignId = a.Id };
                b.Id = this.database.Insert<int>(b);

                var c = new CyclicForeignKeyC { ForeignId = b.Id };
                c.Id = this.database.Insert<int>(c);

                a.ForeignId = c.Id;
                this.database.Update(a);

                var sut = new DataWiper();

                // Act
                sut.ClearAllData(this.database);

                // Assert
                this.database.Count<CyclicForeignKeyA>().Should().Be(0);
                this.database.Count<CyclicForeignKeyB>().Should().Be(0);
                this.database.Count<CyclicForeignKeyC>().Should().Be(0);
            }

            [Fact]
            public void Deletes_data_from_tables_in_other_schemas()
            {
                // Arrange
                var otherId = this.database.Insert<int>(new SchemaOther { Name = "Other" });
                this.database.Insert(new SchemaSimpleForeignKeys { SchemaOtherId = otherId });

                var sut = new DataWiper();

                // Act
                sut.ClearAllData(this.database);

                // Assert
                this.database.Count<SchemaOther>().Should().Be(0);
                this.database.Count<SchemaSimpleForeignKeys>().Should().Be(0);
            }

            [Fact]
            public void Ignores_tables_in_other_schemas()
            {
                // Arrange
                this.database.Insert(new SchemaOther { Name = "Other" });
                var sut = new DataWiper
                    {
                        IgnoredTables = new[]
                            {
                                "Other.SchemaOther"
                            }
                    };

                // Act
                sut.ClearAllData(this.database);

                // Assert
                this.database.Count<SchemaOther>().Should().Be(1);
            }

            [Collection(nameof(SqlServerCollection))]
            public class SqlServer
                : DeleteAllData
            {
                public SqlServer(SqlServerFixture fixture)
                    : base(fixture)
                {
                }
            }
        }
    }
}