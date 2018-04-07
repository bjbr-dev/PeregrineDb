namespace PeregrineDb.PerformanceTests
{
    using System;
    using System.Configuration;
    using System.Data.SqlClient;
    using BenchmarkDotNet.Attributes;
    using BenchmarkDotNet.Attributes.Columns;
    using BenchmarkDotNet.Configs;
    using BenchmarkDotNet.Diagnosers;
    using BenchmarkDotNet.Jobs;
    using BenchmarkDotNet.Order;
    using PeregrineDb.PerformanceTests.Helpers;

    [OrderProvider(SummaryOrderPolicy.FastestToSlowest)]
    [RankColumn]
    [Config(typeof(Config))]
    public abstract class BenchmarkBase
    {
        public const int Iterations = 50;
        protected static readonly Random _rand = new Random();
        protected SqlConnection _connection;
        public static string ConnectionString { get; } = ConfigurationManager.ConnectionStrings["Main"].ConnectionString;
        protected int i;

        protected void BaseSetup()
        {
            this.i = 0;
            this._connection = new SqlConnection(ConnectionString);
            this._connection.Open();
        }

        protected void Step()
        {
            this.i++;
            if (this.i > 5000) this.i = 1;
        }
    }

    public class Config : ManualConfig
    {
        public Config()
        {
            this.Add(new MemoryDiagnoser());
            this.Add(new ORMColum());
            this.Add(new ReturnColum());
            this.Add(Job.Default
                .WithUnrollFactor(BenchmarkBase.Iterations)
                //.WithIterationTime(new TimeInterval(500, TimeUnit.Millisecond))
                .WithLaunchCount(1)
                .WithWarmupCount(0)
                .WithTargetCount(5)
                .WithRemoveOutliers(true)
            );
        }
    }
}