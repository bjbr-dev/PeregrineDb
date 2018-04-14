namespace PeregrineDb.PerformanceTests
{
    using System.Linq;
    using BenchmarkDotNet.Attributes;
    using Dapper;
    using Dapper.Contrib.Extensions;

    public class DapperGetBenchmarks 
        : GetBenchmarks
    {
        [GlobalSetup]
        public void Setup()
        {
            this.BaseSetup();
        }

        [Benchmark(Description = "Query{T} (buffered)")]
        public Post QueryBuffered()
        {
            this.Step();
            return this.Connection.Query<Post>("select * from Posts where Id = @Id", new { Id = this.i }, buffered: true).First();
        }

        [Benchmark(Description = "Query{dynamic} (buffered)")]
        public dynamic QueryBufferedDynamic()
        {
            this.Step();
            return this.Connection.Query("select * from Posts where Id = @Id", new { Id = this.i }, buffered: true).First();
        }

        [Benchmark(Description = "Query{T} (unbuffered)")]
        public Post QueryUnbuffered()
        {
            this.Step();
            return this.Connection.Query<Post>("select * from Posts where Id = @Id", new { Id = this.i }, buffered: false).First();
        }

        [Benchmark(Description = "Query{dynamic} (unbuffered)")]
        public dynamic QueryUnbufferedDynamic()
        {
            this.Step();
            return this.Connection.Query("select * from Posts where Id = @Id", new { Id = this.i }, buffered: false).First();
        }

        [Benchmark(Description = "QueryFirstOrDefault{T}")]
        public Post QueryFirstOrDefault()
        {
            this.Step();
            return this.Connection.QueryFirstOrDefault<Post>("select * from Posts where Id = @Id", new { Id = this.i });
        }

        [Benchmark(Description = "QueryFirstOrDefault{dynamic}")]
        public dynamic QueryFirstOrDefaultDynamic()
        {
            this.Step();
            return this.Connection.QueryFirstOrDefault("select * from Posts where Id = @Id", new { Id = this.i }).First();
        }

        [Benchmark(Description = "Contrib Get{T}")]
        public Post ContribGet()
        {
            this.Step();
            return this.Connection.Get<Post>(this.i);
        }
    }
}
