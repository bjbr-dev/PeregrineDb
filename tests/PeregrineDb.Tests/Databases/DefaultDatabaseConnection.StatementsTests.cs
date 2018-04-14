namespace PeregrineDb.Tests.Databases
{
    using System;
    using System.Collections.Generic;
    using FluentAssertions;
    using PeregrineDb.Dialects;
    using PeregrineDb.Tests.ExampleEntities;
    using PeregrineDb.Tests.Utils;
    using Xunit;

    public abstract class DefaultDatabaseConnectionStatementsTests
    {
        public static IEnumerable<object[]> TestDialects => new[]
            {
                new[] { Dialect.SqlServer2012 },
                new[] { Dialect.PostgreSql }
            };

        public class Execute
            : DefaultDatabaseConnectionStatementsTests
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
    }
}