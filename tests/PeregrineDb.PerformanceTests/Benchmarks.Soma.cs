namespace PeregrineDb.PerformanceTests
{
    using BenchmarkDotNet.Attributes;

    public class SomaBenchmarks : BenchmarkBase
    {
        private dynamic _sdb;

        [GlobalSetup]
        public void Setup()
        {
            this.BaseSetup();
            this._sdb = Simple.Data.Database.OpenConnection(ConnectionString);
        }

        [Benchmark(Description = "FindById")]
        public dynamic QueryDynamic()
        {
            this.Step();
            return this._sdb.Posts.FindById(this.i).FirstOrDefault();
        }
    }
}