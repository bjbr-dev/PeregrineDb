namespace PeregrineDb.PerformanceTests
{
    using System;
    using System.Data.Linq;
    using System.Linq;
    using BenchmarkDotNet.Attributes;
    using PeregrineDb.PerformanceTests.Linq2Sql;

    public class Linq2SqlBenchmarks : BenchmarkBase
    {
        private DataClassesDataContext Linq2SqlContext;
        private static readonly Func<DataClassesDataContext, int, Linq2Sql.Post> compiledQuery =
            CompiledQuery.Compile((DataClassesDataContext ctx, int id) => ctx.Posts.First(p => p.Id == id));

        [GlobalSetup]
        public void Setup()
        {
            this.BaseSetup();
            this.Linq2SqlContext = new DataClassesDataContext(this._connection);
        }

        [Benchmark(Description = "Normal")]
        public Linq2Sql.Post Normal()
        {
            this.Step();
            return this.Linq2SqlContext.Posts.First(p => p.Id == this.i);
        }

        [Benchmark(Description = "Compiled")]
        public Linq2Sql.Post Compiled()
        {
            this.Step();
            return compiledQuery(this.Linq2SqlContext, this.i);
        }

        [Benchmark(Description = "ExecuteQuery")]
        public Post ExecuteQuery()
        {
            this.Step();
            return this.Linq2SqlContext.ExecuteQuery<Post>("select * from Posts where Id = {0}", this.i).First();
        }
    }
}