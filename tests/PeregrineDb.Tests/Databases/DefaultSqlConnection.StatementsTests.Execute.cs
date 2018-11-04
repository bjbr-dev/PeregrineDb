namespace PeregrineDb.Tests.Databases
{
    using System;
    using System.Data;
    using Dapper;
    using FluentAssertions;
    using PeregrineDb.Dialects;
    using PeregrineDb.Tests.Utils;
    using Xunit;
    using Dog = PeregrineDb.Tests.ExampleEntities.Dog;

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
                        act.Should().Throw<Exception>();
                        database.Count<Dog>().Should().Be(0);
                    }
                }
            }

            public class NumRowsAffected
                : Execute
            {
                [Fact]
                public void Returns_0_when_no_count_is_on()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        // Act
                        var result = database.Execute(@"
SET NOCOUNT ON
INSERT INTO dogs (name, age)
VALUES ('Rover', 5);
SET NOCOUNT OFF");

                        // Assert
                        result.NumRowsAffected.Should().Be(-1);
                    }
                }

                [Fact]
                public void Returns_number_of_rows_affected()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        // Act
                        var result = database.Execute(@"
INSERT INTO dogs (name, age)
VALUES ('Rover', 5);
INSERT INTO dogs (name, age)
VALUES ('Rex', 7);");

                        // Assert
                        result.NumRowsAffected.Should().Be(2);
                    }
                }
            }

            public class Parameters
                : Execute
            {
                [Fact]
                public void Executes_command_with_dynamic_parameters()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        // Act
                        database.Execute("INSERT INTO dogs (name, age) VALUES (@Name, @Age)", new { Name = "Rover", Age = 5 });

                        // Assert
                        database.GetAll<Dog>().Should().BeEquivalentTo(new { Name = "Rover", Age = 5 });
                    }
                }

                [Fact]
                public void Does_not_allow_sql_injection_through_dynamic_parameters()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        // Act
                        database.Execute("INSERT INTO dogs (name, age) VALUES (@Name, @Age)", new { Name = "Rover) --", Age = 5 });

                        // Assert
                        database.GetAll<Dog>().Should().BeEquivalentTo(new { Name = "Rover) --", Age = 5 });
                    }
                }

                [Fact(Skip = "Not implemented")]
                public void Can_use_custom_converter_in_interpolated_arguments()
                {
                    ////using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.PostgreSql))
                    ////{
                    ////    // Arrange
                    ////    TypeProvider.AddTypeHandler(new CitextConverter());

                    ////    // Act
                    ////    database.Execute("INSERT INTO parameter_types(typname) SELECT typname FROM pg_type WHERE oid = pg_typeof(@foo) AND typname = 'citext'", new { foo = (Citext)"foo" }).ExpectingAffectedRowCountToBe(1);
                    ////    database.Execute("INSERT INTO parameter_types(typname) SELECT typname FROM pg_type WHERE oid = pg_typeof(@foo) AND typname = 'citext'", new { foo = (Citext)null }).ExpectingAffectedRowCountToBe(1);
                    ////}
                }

                [Fact]
                public void Can_use_output_parameters()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        // Act
                        var p = new DynamicParameters(new { a = 1, b = 2 });
                        p.Add("c", dbType: DbType.Int32, direction: ParameterDirection.Output);

                        database.Execute("set @c = @a + @b", p);

                        // Assert
                        p.Get<int>("@c").Should().Be(3);
                    }
                }

                [Fact]
                public void Output_parameter_can_be_set_to_null()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        // Act
                        var p = new DynamicParameters();
                        p.Add("@b", dbType: DbType.Int32, direction: ParameterDirection.Output);
                        database.Execute("select @b = null", p);

                        // Assert
                        p.Get<int?>("@b").Should().BeNull();
                    }
                }
            }
        }
    }
}