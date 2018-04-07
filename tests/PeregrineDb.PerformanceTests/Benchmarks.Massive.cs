namespace PeregrineDb.PerformanceTests
{
    using System.Linq;
    using BenchmarkDotNet.Attributes;
    using PeregrineDb.PerformanceTests.Massive;

    public class MassiveBenchmarks : BenchmarkBase
    {
        private DynamicModel _model;

        [GlobalSetup]
        public void Setup()
        {
            this.BaseSetup();
            this._model = new DynamicModel(ConnectionString);
        }

        [Benchmark(Description = "Query (dynamic)")]
        public dynamic QueryDynamic()
        {
            this.Step();
            return this._model.Query("select * from Posts where Id = @0", this._connection, this.i).First();
        }
    }
}