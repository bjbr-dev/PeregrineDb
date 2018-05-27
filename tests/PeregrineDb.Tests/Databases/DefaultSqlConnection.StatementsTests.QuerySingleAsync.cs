namespace PeregrineDb.Tests.Databases
{
    using System.Threading.Tasks;
    using PeregrineDb.Tests.Utils;
    using Xunit;

    public abstract partial class DefaultDatabaseConnectionStatementsTests
    {
        public abstract class QuerySingleAsync
            : DefaultDatabaseConnectionStatementsTests
        {
            [Fact]
            public async Task TestBasicStringUsageQuerySingleAsync()
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                {
                    var str = await database.QuerySingleAsync<string>("select \'abc\' as [Value]").ConfigureAwait(false);
                    Assert.Equal("abc", str);
                }
            }
        }
    }
}