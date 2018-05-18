namespace PeregrineDb.Tests.Databases
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Linq;
    using FluentAssertions;
    using PeregrineDb.Dialects;
    using PeregrineDb.Mapping;
    using PeregrineDb.Tests.ExampleEntities;
    using PeregrineDb.Tests.Utils;
    using Xunit;

    public abstract partial class DefaultDatabaseConnectionStatementsTests
    {
        public abstract class Execute
            : DefaultDatabaseConnectionStatementsTests
        {
            public class Transactions
                : Execute
            {
                [Theory]
                [MemberData(nameof(TestDialects))]
                public void Can_execute_inside_a_transaction(IDialect dialect)
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                    {
                        // Act
                        using (var unitOfWork = database.StartUnitOfWork())
                        {
                            var command = dialect.MakeInsertCommand(new Dog { Name = "Foo", Age = 4 });
                            unitOfWork.Execute(in command);

                            unitOfWork.SaveChanges();
                        }

                        // Assert
                        database.Count<Dog>().Should().Be(1);
                    }
                }

                [Theory]
                [MemberData(nameof(TestDialects))]
                public void Rollsback_changes_when_transaction_is_disposed_without_saving(IDialect dialect)
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                    {
                        // Act
                        using (var unitOfWork = database.StartUnitOfWork())
                        {
                            var command = dialect.MakeInsertCommand(new Dog { Name = "Foo", Age = 4 });
                            unitOfWork.Execute(in command);
                        }

                        // Assert
                        database.Count<Dog>().Should().Be(0);
                    }
                }

                [Theory]
                [MemberData(nameof(TestDialects))]
                public void Rollsback_changes_when_exception_is_thrown_in_transaction(IDialect dialect)
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                    {
                        // Act
                        Action act = () =>
                        {
                            using (var unitOfWork = database.StartUnitOfWork())
                            {
                                var command = dialect.MakeInsertCommand(new Dog { Name = "Foo", Age = 4 });
                                unitOfWork.Execute(in command);

                                throw new Exception();
                            }
                        };

                        // Assert
                        act.ShouldThrow<Exception>();
                        database.Count<Dog>().Should().Be(0);
                    }
                }
            }

            public class SingleCommand
                : Execute
            {
                [Fact]
                public void Returns_0_when_no_count_is_on()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        var result = database.Execute($@"
SET NOCOUNT ON
INSERT INTO dogs (name, age)
VALUES ('Rover', 5);
SET NOCOUNT OFF");
                        result.NumRowsAffected.Should().Be(-1);
                    }
                }

                [Fact]
                public void Returns_number_of_rows_affected()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        var result = database.Execute($@"
INSERT INTO dogs (name, age)
VALUES ('Rover', 5);
INSERT INTO dogs (name, age)
VALUES ('Rex', 7);");

                        result.NumRowsAffected.Should().Be(2);
                    }
                }
            }

            public class MultipleCommands
                : Execute
            {
                [Fact]
                public void TestExecuteMultipleCommand()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        database.Execute($"create table #t(i int)");
                        try
                        {
                            var command = new SqlCommand("insert #t (i) values(@a)", new[] { new { a = 1 }, new { a = 2 }, new { a = 3 }, new { a = 4 } });

                            var tally = database.Execute(in command);
                            var sum = database.Query<int>($"select sum(i) from #t").First();
                            Assert.Equal(4, tally.NumRowsAffected);
                            Assert.Equal(10, sum);
                        }
                        finally
                        {
                            database.Execute($"drop table #t");
                        }
                    }
                }

                private class Student
                {
                    public string Name { get; set; }

                    public int Age { get; set; }
                }

                [Fact]
                public void TestExecuteMultipleCommandStrongType()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        database.Execute($"create table #t(Name nvarchar(max), Age int)");
                        try
                        {
                            var command = new SqlCommand($"insert #t (Name,Age) values(@Name, @Age)", new List<Student>
                                {
                                    new Student { Age = 1, Name = "sam" },
                                    new Student { Age = 2, Name = "bob" }
                                });
                            var tally = database.Execute(in command);
                            int sum = database.Query<int>($"select sum(Age) from #t").First();
                            Assert.Equal(2, tally.NumRowsAffected);
                            Assert.Equal(3, sum);
                        }
                        finally
                        {
                            database.Execute($"drop table #t");
                        }
                    }
                }

                [Fact]
                public void TestExecuteMultipleCommandObjectArray()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        database.Execute($"create table #t(i int)");
                        var command = new SqlCommand("insert #t (i) values(@a)", new object[]
                            {
                                new { a = 1 },
                                new { a = 2 },
                                new { a = 3 },
                                new { a = 4 }
                            });
                        var tally = database.Execute(in command);
                        var sum = database.Query<int>($"select sum(i) from #t drop table #t").First();
                        tally.NumRowsAffected.Should().Be(4);
                        sum.Should().Be(10);
                    }
                }
            }

            public class Parameters
                : Execute
            {
                [Fact]
                public void TestExecuteCommandWithHybridParameters()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        var p = new DynamicParameters(new { a = 1, b = 2 });
                        p.Add("c", dbType: DbType.Int32, direction: ParameterDirection.Output);

                        var sqlCommand = new SqlCommand("set @c = @a + @b", p);
                        database.Execute(in sqlCommand);
                        Assert.Equal(3, p.Get<int>("@c"));
                    }
                }

                [Fact]
                public void TestDynamicParamNullSupport()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        var p = new DynamicParameters();

                        p.Add("@b", dbType: DbType.Int32, direction: ParameterDirection.Output);
                        var command = new SqlCommand("select @b = null", p);
                        database.Execute(in command);

                        Assert.Null(p.Get<int?>("@b"));
                    }
                }
            }
        }
    }
}