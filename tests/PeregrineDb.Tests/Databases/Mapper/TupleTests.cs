namespace PeregrineDb.Tests.Databases.Mapper
{
    using System;
    using PeregrineDb.Tests.Utils;
    using Xunit;

    public class TupleTests
    {
        [Fact]
        public void TupleStructParameter_Fails_HelpfulMessage()
        {
            using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
            {
                var command = new SqlCommand($"select @id", (id: 42, name: "Fred"));
                var ex = Assert.Throws<NotSupportedException>(() => database.QuerySingle<int>(in command));
                Assert.Equal(
                    "ValueTuple should not be used for parameters - the language-level names are not available to use as parameter names, and it adds unnecessary boxing",
                    ex.Message);
            }
        }

        [Fact]
        public void TupleClassParameter_Works()
        {
            using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
            {
                var command = new SqlCommand("select @Item1", Tuple.Create(42, "Fred"));
                Assert.Equal(42, database.QuerySingle<int>(in command));
            }
        }

        [Fact]
        public void TupleReturnValue_Works_ByPosition()
        {
            using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
            {
                var val = database.QuerySingle<(int id, string name)>($"select 42, 'Fred'");
                Assert.Equal(42, val.id);
                Assert.Equal("Fred", val.name);
            }
        }

        [Fact]
        public void TupleReturnValue_TooManyColumns_Ignored()
        {
            using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
            {
                var val = database.QuerySingle<(int id, string name)>($"select 42, 'Fred', 123");
                Assert.Equal(42, val.id);
                Assert.Equal("Fred", val.name);
            }
        }

        [Fact]
        public void TupleReturnValue_TooFewColumns_Unmapped()
        {
            using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
            {
                // I'm very wary of making this throw, but I can also see some sense in pointing out the oddness
                var val = database.QuerySingle<(int id, string name, int extra)>($"select 42, 'Fred'");
                Assert.Equal(42, val.id);
                Assert.Equal("Fred", val.name);
                Assert.Equal(0, val.extra);
            }
        }

        [Fact]
        public void TupleReturnValue_Works_NamesIgnored()
        {
            using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
            {
                var val = database.QuerySingle<(int id, string name)>($"select 42 as [Item2], 'Fred' as [Item1]");
                Assert.Equal(42, val.id);
                Assert.Equal("Fred", val.name);
            }
        }
    }
}
