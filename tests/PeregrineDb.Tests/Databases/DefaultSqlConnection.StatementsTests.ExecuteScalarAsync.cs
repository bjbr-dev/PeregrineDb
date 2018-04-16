namespace PeregrineDb.Tests.Databases
{
    using System.Threading.Tasks;
    using PeregrineDb.Tests.Utils;
    using Xunit;

    public abstract partial class DefaultDatabaseConnectionStatementsTests
    {
        public abstract class ExecuteScalarAsync
            : DefaultDatabaseConnectionStatementsTests
        {
            [Fact]
            public async Task Issue22_ExecuteScalarAsync()
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                {
                    var i = await database.ExecuteScalarAsync<int>($"select 123").ConfigureAwait(false);
                    Assert.Equal(123, i);

                    i = await database.ExecuteScalarAsync<int>($"select cast(123 as bigint)").ConfigureAwait(false);
                    Assert.Equal(123, i);

                    var j = await database.ExecuteScalarAsync<long>($"select 123").ConfigureAwait(false);
                    Assert.Equal(123L, j);

                    j = await database.ExecuteScalarAsync<long>($"select cast(123 as bigint)").ConfigureAwait(false);
                    Assert.Equal(123L, j);

                    var k = await database.ExecuteScalarAsync<int?>($"select {default(int?)}").ConfigureAwait(false);
                    Assert.Null(k);
                }
            }
        }
    }
}