namespace PeregrineDb.Tests.Databases
{
    using System;
    using System.Threading.Tasks;
    using FluentAssertions;
    using PeregrineDb.Dialects;
    using PeregrineDb.Tests.Utils;
    using Xunit;
    using Dog = PeregrineDb.Tests.ExampleEntities.Dog;

    public abstract partial class DefaultDatabaseConnectionStatementsTests
    {
        public abstract class ExecuteAsync
            : DefaultDatabaseConnectionStatementsTests
        {
            public class Transactions
                : ExecuteAsync
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

            [Fact]
            public async Task TestExecuteAsync()
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                {
                    var val = await database.ExecuteAsync("declare @foo table(id int not null); insert @foo values(@value);", new { value = 1 }).ConfigureAwait(false);
                    val.ExpectingAffectedRowCountToBe(1);
                }
            }
        }
    }
}