namespace PeregrineDb.Tests.Databases
{
    using System;
    using PeregrineDb.Tests.Utils;
    using Xunit;

    public abstract partial class DefaultDatabaseConnectionStatementsTests
    {
        public class QuerySingle
            : DefaultDatabaseConnectionStatementsTests
        {
            [Fact]
            public void Issue601_InternationalParameterNamesWork()
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                {
                    // regular parameter
                    var result = database.RawQuerySingle<int>("select @æøå٦", new { æøå٦ = 42 });
                    Assert.Equal(42, result);
                }
            }

            [Fact]
            public void Test_Single_First_Default()
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                {
                    FormattableString sql = $"select 0 where 1 = 0;"; // no rows

                    var ex = Assert.Throws<InvalidOperationException>(() => database.QueryFirst<int>(sql));
                    Assert.Equal("Sequence contains no elements", ex.Message);

                    ex = Assert.Throws<InvalidOperationException>(() => database.QuerySingle<int>(sql));
                    Assert.Equal("Sequence contains no elements", ex.Message);

                    Assert.Equal(0, database.QueryFirstOrDefault<int>(sql));
                    Assert.Equal(0, database.QuerySingleOrDefault<int>(sql));

                    sql = $"select 1;"; // one row
                    Assert.Equal(1, database.QueryFirst<int>(sql));
                    Assert.Equal(1, database.QuerySingle<int>(sql));
                    Assert.Equal(1, database.QueryFirstOrDefault<int>(sql));
                    Assert.Equal(1, database.QuerySingleOrDefault<int>(sql));

                    sql = $"select 2 union select 3 order by 1;"; // two rows
                    Assert.Equal(2, database.QueryFirst<int>(sql));

                    ex = Assert.Throws<InvalidOperationException>(() => database.QuerySingle<int>(sql));
                    Assert.Equal("Sequence contains more than one element", ex.Message);

                    Assert.Equal(2, database.QueryFirstOrDefault<int>(sql));

                    ex = Assert.Throws<InvalidOperationException>(() => database.QuerySingleOrDefault<int>(sql));
                    Assert.Equal("Sequence contains more than one element", ex.Message);
                }
            }

            [Fact]
            public void GetOnlyProperties()
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                {
                    var obj = database.QuerySingle<HazGetOnly>($"select 42 as [Id], 'def' as [Name];");
                    Assert.Equal(42, obj.Id);
                    Assert.Equal("def", obj.Name);
                }
            }

            private class HazGetOnly
            {
                public int Id { get; }
                public string Name { get; } = "abc";
            }
        }
    }
}