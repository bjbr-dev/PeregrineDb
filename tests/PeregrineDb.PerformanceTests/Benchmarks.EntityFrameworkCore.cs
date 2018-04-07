namespace PeregrineDb.PerformanceTests
{
    using System;
    using System.Data.Linq;
    using System.Linq;
    using BenchmarkDotNet.Attributes;
    using Microsoft.EntityFrameworkCore;
    using PeregrineDb.PerformanceTests.Linq2Sql;

    public class EFCoreBenchmarks : BenchmarkBase
    {
        private EntityFrameworkCore.EFCoreContext Context;
        private static readonly Func<DataClassesDataContext, int, Linq2Sql.Post> compiledQuery =
            CompiledQuery.Compile((DataClassesDataContext ctx, int id) => ctx.Posts.First(p => p.Id == id));

        [GlobalSetup]
        public void Setup()
        {
            this.BaseSetup();
            this.Context = new EntityFrameworkCore.EFCoreContext(this._connection.ConnectionString);
        }

        [Benchmark(Description = "Normal")]
        public Post Normal()
        {
            this.Step();
            return this.Context.Posts.First(p => p.Id == this.i);
        }

        [Benchmark(Description = "SqlQuery")]
        public Post SqlQuery()
        {
            this.Step();
            return this.Context.Posts.FromSql("select * from Posts where Id = {0}", this.i).First();
        }

        [Benchmark(Description = "No Tracking")]
        public Post NoTracking()
        {
            this.Step();
            return this.Context.Posts.AsNoTracking().First(p => p.Id == this.i);
        }
    }
}