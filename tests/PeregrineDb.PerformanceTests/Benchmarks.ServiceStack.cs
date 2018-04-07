namespace PeregrineDb.PerformanceTests
{
    using System.Data;
    using BenchmarkDotNet.Attributes;
    using ServiceStack.OrmLite;

    public class ServiceStackBenchmarks : BenchmarkBase
    {
        private IDbConnection _db;

        [GlobalSetup]
        public void Setup()
        {
            this.BaseSetup();
            var dbFactory = new OrmLiteConnectionFactory(ConnectionString, SqlServerDialect.Provider);
            this._db = dbFactory.Open();
        }

        [Benchmark(Description = "SingleById")]
        public Post Query()
        {
            this.Step();
            return this._db.SingleById<Post>(this.i);
        }
    }
}