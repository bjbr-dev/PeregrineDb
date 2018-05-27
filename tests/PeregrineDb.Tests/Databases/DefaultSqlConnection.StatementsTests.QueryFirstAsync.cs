namespace PeregrineDb.Tests.Databases
{
    using System.Threading.Tasks;
    using PeregrineDb.Tests.Utils;
    using Xunit;

    public abstract partial class DefaultDatabaseConnectionStatementsTests
    {
        public abstract class QueryFirstAsync
            : DefaultDatabaseConnectionStatementsTests
        {
            [Fact]
            public async Task TestBasicStringUsageQueryFirstAsync()
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                {
                    Assert.Equal("abc", await database.QueryFirstAsync<string>("select 'abc' as [Value] union all select @value", new { value = "def" }).ConfigureAwait(false));
                }
            }
        }
    }
}