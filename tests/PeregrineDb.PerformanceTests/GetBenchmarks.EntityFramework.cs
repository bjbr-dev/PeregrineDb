namespace PeregrineDb.PerformanceTests
{
    using System.Linq;
    using BenchmarkDotNet.Attributes;

    public class Ef6GetBenchmarks : GetBenchmarks
    {
        private EntityFramework.EFContext Context;

        [GlobalSetup]
        public void Setup()
        {
            this.BaseSetup();
            this.Context = new EntityFramework.EFContext(this.Connection);
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
            return this.Context.Database.SqlQuery<Post>("select * from Posts where Id = {0}", this.i).First();
        }

        [Benchmark(Description = "No Tracking")]
        public Post NoTracking()
        {
            this.Step();
            return this.Context.Posts.AsNoTracking().First(p => p.Id == this.i);
        }
    }
}