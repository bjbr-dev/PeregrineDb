namespace PeregrineDb.Tests.Databases
{
    using PeregrineDb.Tests.Utils;
    using Xunit;

    public abstract partial class DefaultDatabaseConnectionStatementsTests
    {
        public class ExecuteScalar
            : DefaultDatabaseConnectionStatementsTests
        {
            [Fact]
            public void Issue22_ExecuteScalar()
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                {
                    Assert.Equal(123, database.ExecuteScalar<int>("select 123"));
                    Assert.Equal(123, database.ExecuteScalar<int>("select cast(123 as bigint)"));
                    Assert.Equal(123L, database.ExecuteScalar<long>("select 123"));
                    Assert.Equal(123L, database.ExecuteScalar<long>("select cast(123 as bigint)"));
                    Assert.Null(database.ExecuteScalar<int?>("select @value", new { value = default(int?) }));
                }
            }
        }
    }
}