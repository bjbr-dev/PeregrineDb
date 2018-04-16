namespace PeregrineDb.Tests.Databases
{
    using System;
    using System.Data.SqlClient;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using PeregrineDb.Databases.Mapper;
    using PeregrineDb.Tests.Databases.Mapper.SharedTypes;
    using PeregrineDb.Tests.Utils;
    using Xunit;

    public abstract partial class DefaultDatabaseConnectionStatementsTests
    {
        public class QueryAsync
            : DefaultDatabaseConnectionStatementsTests
        {
            [Fact]
            public async Task TestBasicStringUsageAsync()
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                {
                    var query = await database.QueryAsync<string>($"select 'abc' as [Value] union all select {"def"}")
                                              .ConfigureAwait(false);
                    Assert.Equal(new[] { "abc", "def" }, query.ToArray());
                }
            }

            [Fact]
            public void TestLongOperationWithCancellation()
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                {
                    var cancel = new CancellationTokenSource(TimeSpan.FromSeconds(5));
                    var task = database.QueryAsync<int>($"waitfor delay '00:00:10';select 1", cancellationToken: cancel.Token);
                    try
                    {
                        if (!task.Wait(TimeSpan.FromSeconds(7)))
                        {
                            throw new TimeoutException(); // should have cancelled
                        }
                    }
                    catch (AggregateException agg)
                    {
                        Assert.True(agg.InnerException is SqlException);
                    }
                }
            }

            [Fact]
            public async Task TestClassWithStringUsageAsync()
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                {
                    var query = await database.QueryAsync<BasicType>($"select 'abc' as [Value] union all select {"def"}")
                                              .ConfigureAwait(false);
                    Assert.Equal(new[] { "abc", "def" }, query.Select(x => x.Value));
                }
            }

            [Fact]
            public async Task Issue346_QueryAsyncConvert()
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                {
                    int i = (await database.QueryAsync<int>($"Select Cast(123 as bigint)").ConfigureAwait(false)).First();
                    Assert.Equal(123, i);
                }
            }

            private class BasicType
            {
                public string Value { get; set; }
            }

            [Fact]
            public async Task TestSubsequentQueriesSuccessAsync()
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                {
                    var data0 = (await database.QueryAsync<AsyncFoo0>($"select 1 as [Id] where 1 = 0").ConfigureAwait(false)).ToList();
                    Assert.Empty(data0);

                    data0 = (await database.QueryAsync<AsyncFoo0>($"select 1 as [Id] where 1 = 0").ConfigureAwait(false)).ToList();
                    Assert.Empty(data0);
                }
            }


            private class AsyncFoo0 { public int Id { get; set; } }

            [Fact]
            public async Task TestAtEscaping()
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                {
                    var id = (await database.QueryAsync<int>($@"
declare @@Name int
select @@Name = {1}+1
select @@Name
                ").ConfigureAwait(false)).Single();
                    Assert.Equal(2, id);
                }
            }

            [Fact]
            public async Task Issue563_QueryAsyncShouldThrowException()
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                {
                    try
                    {
                        var data = (await database.QueryAsync<int>($"select 1 union all select 2; RAISERROR('after select', 16, 1);").ConfigureAwait(false))
                            .ToList();
                        Assert.True(false, "Expected Exception");
                    }
                    catch (SqlException ex) when (ex.Message == "after select")
                    {
                        /* swallow only this */
                    }
                }
            }

            [Fact]
            public async void SO35470588_WrongValuePidValue()
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                {
                    // nuke, rebuild, and populate the table
                    try
                    {
                        database.Execute($"drop table TPTable");
                    }
                    catch
                    {
                        /* don't care */
                    }

                    database.Execute($@"
create table TPTable (Pid int not null primary key identity(1,1), Value int not null);
insert TPTable (Value) values (2), (568)");

                    // fetch the data using the query in the question, then force to a dictionary
                    var rows = (await database.QueryAsync<TPTable>($"select * from TPTable").ConfigureAwait(false))
                        .ToDictionary(x => x.Pid);

                    // check the number of rows
                    Assert.Equal(2, rows.Count);

                    // check row 1
                    var row = rows[1];
                    Assert.Equal(1, row.Pid);
                    Assert.Equal(2, row.Value);

                    // check row 2
                    row = rows[2];
                    Assert.Equal(2, row.Pid);
                    Assert.Equal(568, row.Value);
                }
            }

            public class TPTable
            {
                public int Pid { get; set; }
                public int Value { get; set; }
            }
        }
    }
}