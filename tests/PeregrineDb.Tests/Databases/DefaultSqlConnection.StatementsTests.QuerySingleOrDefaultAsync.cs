namespace PeregrineDb.Tests.Databases
{
    using System.Threading.Tasks;
    using PeregrineDb.Tests.Utils;
    using Xunit;

    public abstract partial class DefaultDatabaseConnectionStatementsTests
    {
        public abstract class QuerySingleOrDefaultAsync
            : DefaultDatabaseConnectionStatementsTests
        {
            [Fact]
            public async Task TestBasicStringUsageQuerySingleOrDefaultAsync()
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                {
                    var str = await database.QuerySingleOrDefaultAsync<string>($"select null as [Value]").ConfigureAwait(false);
                    Assert.Null(str);
                }
            }
        }
    }
}