namespace PeregrineDb.PerformanceTests
{
    using System.Configuration;
    using System.Data.SqlClient;
    using BenchmarkDotNet.Attributes;
    using BenchmarkDotNet.Attributes.Columns;
    using BenchmarkDotNet.Configs;
    using BenchmarkDotNet.Diagnosers;
    using BenchmarkDotNet.Exporters.Json;
    using BenchmarkDotNet.Jobs;
    using BenchmarkDotNet.Order;
    using PeregrineDb.PerformanceTests.Helpers;

    [OrderProvider(SummaryOrderPolicy.FastestToSlowest)]
    [RankColumn]
    [Config(typeof(Config))]
    public abstract class GetBenchmarks
    {
        protected SqlConnection Connection;
        public static string ConnectionString { get; } = ConfigurationManager.ConnectionStrings["Main"].ConnectionString;
        protected int i;

        protected void BaseSetup()
        {
            this.i = 0;
            this.Connection = new SqlConnection(ConnectionString);
            this.Connection.Open();
        }

        protected void Step()
        {
            this.i++;
            if (this.i > 5000)
            {
                this.i = 1;
            }
        }

        class Config
            : ManualConfig
        {
            public Config()
            {
                this.Add(JsonExporter.Full);
                this.Add(new MemoryDiagnoser());
                this.Add(new OrmColum());
                this.Add(Job.Default
                            .WithUnrollFactor(50)
                            .WithLaunchCount(1)
                            .WithWarmupCount(0)
                            .WithTargetCount(5)
                            .WithRemoveOutliers(true)
                );
            }
        }
    }
}