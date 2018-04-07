namespace PeregrineDb.PerformanceTests
{
    using System.Linq;
    using BenchmarkDotNet.Attributes;
    using Dapper;
    using Dapper.Contrib.Extensions;

    public class DapperBenchmarks : BenchmarkBase
    {
        [GlobalSetup]
        public void Setup()
        {
            this.BaseSetup();
        }

        [Benchmark(Description = "Query<T> (buffered)")]
        public Post QueryBuffered()
        {
            this.Step();
            return this._connection.Query<Post>("select * from Posts where Id = @Id", new { Id = this.i }, buffered: true).First();
        }

        [Benchmark(Description = "Query<dyanmic> (buffered)")]
        public dynamic QueryBufferedDynamic()
        {
            this.Step();
            return this._connection.Query("select * from Posts where Id = @Id", new { Id = this.i }, buffered: true).First();
        }

        [Benchmark(Description = "Query<T> (unbuffered)")]
        public Post QueryUnbuffered()
        {
            this.Step();
            return this._connection.Query<Post>("select * from Posts where Id = @Id", new { Id = this.i }, buffered: false).First();
        }

        [Benchmark(Description = "Query<dyanmic> (unbuffered)")]
        public dynamic QueryUnbufferedDynamic()
        {
            this.Step();
            return this._connection.Query("select * from Posts where Id = @Id", new { Id = this.i }, buffered: false).First();
        }

        [Benchmark(Description = "QueryFirstOrDefault<T>")]
        public Post QueryFirstOrDefault()
        {
            this.Step();
            return this._connection.QueryFirstOrDefault<Post>("select * from Posts where Id = @Id", new { Id = this.i });
        }

        [Benchmark(Description = "QueryFirstOrDefault<dyanmic>")]
        public dynamic QueryFirstOrDefaultDynamic()
        {
            this.Step();
            return this._connection.QueryFirstOrDefault("select * from Posts where Id = @Id", new { Id = this.i }).First();
        }

        [Benchmark(Description = "Contrib Get<T>")]
        public Post ContribGet()
        {
            this.Step();
            return this._connection.Get<Post>(this.i);
        }
    }
}
