namespace PeregrineDb.Tests.Databases
{
    using System.Threading.Tasks;
    using PeregrineDb.Tests.ExampleEntities;
    using PeregrineDb.Tests.Utils;
    using Xunit;

    public abstract partial class DefaultDatabaseConnectionStatementsTests
    {
        public abstract class QueryFirstOrDefaultAsync
            : DefaultDatabaseConnectionStatementsTests
        {
            [Fact]
            public async Task TestBasicStringUsageQueryFirstOrDefaultAsync()
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                {
                    var str = await database.QueryFirstOrDefaultAsync<string>("select null as [Value] union all select @value", new { value = "def" }).ConfigureAwait(false);
                    Assert.Null(str);
                }
            }

            [Fact]
            public async Task TestSchemaChangedViaFirstOrDefaultAsync()
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                {
                    await database.ExecuteAsync("create table #dog(Age int, Name nvarchar(max)) insert #dog values(1, \'Alf\')").ConfigureAwait(false);
                    try
                    {
                        var d = await database.QueryFirstOrDefaultAsync<Dog>("select * from #dog").ConfigureAwait(false);
                        Assert.Equal("Alf", d.Name);
                        Assert.Equal(1, d.Age);
                        database.Execute("alter table #dog drop column Name");
                        d = await database.QueryFirstOrDefaultAsync<Dog>("select * from #dog").ConfigureAwait(false);
                        Assert.Null(d.Name);
                        Assert.Equal(1, d.Age);
                    }
                    finally
                    {
                        await database.ExecuteAsync("drop table #dog").ConfigureAwait(false);
                    }
                }
            }
        }
    }
}