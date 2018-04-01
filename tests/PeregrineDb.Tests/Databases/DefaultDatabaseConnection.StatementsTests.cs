namespace PeregrineDb.Tests.Databases
{
    using System.Collections.Generic;
    using System.Data.SqlClient;
    using FluentAssertions;
    using PeregrineDb.Dialects;
    using PeregrineDb.SqlCommands;
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

        public static IEnumerable<object[]> TestDialectsWithData(string data) => new[]
            {
                new object[] { Dialect.SqlServer2012, data },
                new object[] { Dialect.PostgreSql, data }
            };

        public class Execute
            : DefaultDatabaseConnectionStatementsTests
        {
            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Can_execute_inside_a_transaction(IDialect dialect)
            {
                using (var instance = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    var database = instance.Item;

                    // Act
                    using (var unitOfWork = database.StartUnitOfWork())
                    {
                        var schema = database.Config.MakeSchema<User>();
                        var sql = dialect.MakeInsertStatement(schema, new User { Name = "Foo", Age = 4 });
                        unitOfWork.Execute(sql);

                        unitOfWork.SaveChanges();
                    }

                    // Assert
                    database.Count<User>().Should().Be(1);
                }
            }
        }
    }
}