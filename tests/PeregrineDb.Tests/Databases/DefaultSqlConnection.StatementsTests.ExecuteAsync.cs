namespace PeregrineDb.Tests.Databases
{
    using System;
    using System.Threading.Tasks;
    using FluentAssertions;
    using PeregrineDb.Databases.Mapper;
    using PeregrineDb.Dialects;
    using PeregrineDb.Tests.Databases.Mapper.SharedTypes;
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

            [Fact]
            public async Task TestExecuteAsync()
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                {
                    var val = await database.ExecuteAsync($"declare @foo table(id int not null); insert @foo values({1});")
                                            .ConfigureAwait(false);
                    val.Equals(1);
                }
            }

            [Fact]
            public async Task TestSupportForDynamicParametersOutputExpressionsAsync()
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))

                {
                    var bob = new Person { Name = "bob", PersonId = 1, Address = new Address { PersonId = 2 } };

                    var p = new DynamicParameters(bob);
                    p.Output(bob, b => b.PersonId);
                    p.Output(bob, b => b.Occupation);
                    p.Output(bob, b => b.NumberOfLegs);
                    p.Output(bob, b => b.Address.Name);
                    p.Output(bob, b => b.Address.PersonId);

                    var sqlCommand = new SqlCommand($@"
SET @Occupation = 'grillmaster' 
SET @PersonId = @PersonId + 1 
SET @NumberOfLegs = @NumberOfLegs - 1
SET @AddressName = 'bobs burgers'
SET @AddressPersonId = @PersonId", p);
                    await database.ExecuteAsync(sqlCommand).ConfigureAwait(false);

                    Assert.Equal("grillmaster", bob.Occupation);
                    Assert.Equal(2, bob.PersonId);
                    Assert.Equal(1, bob.NumberOfLegs);
                    Assert.Equal("bobs burgers", bob.Address.Name);
                    Assert.Equal(2, bob.Address.PersonId);
                }
            }
        }
    }
}