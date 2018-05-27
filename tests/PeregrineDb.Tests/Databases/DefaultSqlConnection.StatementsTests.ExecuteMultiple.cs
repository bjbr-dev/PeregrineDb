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
        public abstract class ExecuteMultiple
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
                            unitOfWork.Execute(command.CommandText, command.Parameters);

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
                            unitOfWork.Execute(command.CommandText, command.Parameters);
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
                                unitOfWork.Execute(command.CommandText, command.Parameters);

                                throw new Exception();
                            }
                        };

                        // Assert
                        act.ShouldThrow<Exception>();
                        database.Count<Dog>().Should().Be(0);
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
                        database.Execute("create table #t(i int)");
                        try
                        {
                            var tally = database.ExecuteMultiple("insert #t (i) values(@a)", new[] { new { a = 1 }, new { a = 2 }, new { a = 3 }, new { a = 4 } });
                            var sum = database.Query<int>("select sum(i) from #t").First();
                            Assert.Equal(4, tally.NumRowsAffected);
                            Assert.Equal(10, sum);
                        }
                        finally
                        {
                            database.Execute("drop table #t");
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
                        database.Execute("create table #t(Name nvarchar(max), Age int)");
                        try
                        {
                            var tally = database.ExecuteMultiple("insert #t (Name,Age) values(@Name, @Age)", new List<Student>
                                {
                                    new Student { Age = 1, Name = "sam" },
                                    new Student { Age = 2, Name = "bob" }
                                });

                            var sum = database.Query<int>("select sum(Age) from #t").First();
                            Assert.Equal(2, tally.NumRowsAffected);
                            Assert.Equal(3, sum);
                        }
                        finally
                        {
                            database.Execute("drop table #t");
                        }
                    }
                }

                [Fact]
                public void TestExecuteMultipleCommandObjectArray()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        database.Execute("create table #t(i int)");
                        var tally = database.ExecuteMultiple("insert #t (i) values(@a)", new object[]
                            {
                                new { a = 1 },
                                new { a = 2 },
                                new { a = 3 },
                                new { a = 4 }
                            });
                        var sum = database.Query<int>("select sum(i) from #t drop table #t").First();
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

                        database.Execute("set @c = @a + @b", p);
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
                        database.Execute("select @b = null", p);

                        Assert.Null(p.Get<int?>("@b"));
                    }
                }
            }
        }
    }
}