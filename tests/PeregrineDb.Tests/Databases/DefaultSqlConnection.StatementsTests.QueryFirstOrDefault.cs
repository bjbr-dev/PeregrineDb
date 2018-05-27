namespace PeregrineDb.Tests.Databases
{
    using PeregrineDb.Tests.ExampleEntities;
    using PeregrineDb.Tests.SharedTypes;
    using PeregrineDb.Tests.Utils;
    using Xunit;

    public abstract partial class DefaultDatabaseConnectionStatementsTests
    {
        public class QueryFirstOrDefault
            : DefaultDatabaseConnectionStatementsTests
        {
            [Fact]
            public void TestSchemaChangedViaFirstOrDefault()
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                {
                    database.Execute("create table #dog(Age int, Name nvarchar(max)) insert #dog values(1, \'Alf\')");
                    try
                    {
                        var d = database.QueryFirstOrDefault<Dog>("select * from #dog");
                        Assert.Equal("Alf", d.Name);
                        Assert.Equal(1, d.Age);
                        database.Execute("alter table #dog drop column Name");
                        d = database.QueryFirstOrDefault<Dog>("select * from #dog");
                        Assert.Null(d.Name);
                        Assert.Equal(1, d.Age);
                    }
                    finally
                    {
                        database.Execute("drop table #dog");
                    }
                }
            }
        }
    }
}