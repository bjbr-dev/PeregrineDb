namespace PeregrineDb.PerformanceTests
{
    using System.Data;
    using System.Linq;
    using BenchmarkDotNet.Attributes;
    using Susanoo;
    using Susanoo.Processing;

    public class SusanooBenchmarks : BenchmarkBase
    {
        private DatabaseManager _db;
        private static readonly ISingleResultSetCommandProcessor<dynamic, Post> _cmd =
                CommandManager.Instance.DefineCommand("SELECT * FROM Posts WHERE Id = @Id", CommandType.Text)
                    .DefineResults<Post>()
                    .Realize();
        private static readonly ISingleResultSetCommandProcessor<dynamic, dynamic> _cmdDynamic =
                CommandManager.Instance.DefineCommand("SELECT * FROM Posts WHERE Id = @Id", CommandType.Text)
                    .DefineResults<dynamic>()
                    .Realize();

        [GlobalSetup]
        public void Setup()
        {
            this.BaseSetup();
            this._db = new DatabaseManager(this._connection);
        }

        [Benchmark(Description = "Mapping Cache")]
        public Post MappingCache()
        {
            this.Step();
            return CommandManager.Instance.DefineCommand("SELECT * FROM Posts WHERE Id = @Id", CommandType.Text)
                    .DefineResults<Post>()
                    .Realize()
                    .Execute(this._db, new { Id = this.i }).First();
        }

        [Benchmark(Description = "Mapping Cache (dynamic)")]
        public dynamic MappingCacheDynamic()
        {
            this.Step();
            return CommandManager.Instance.DefineCommand("SELECT * FROM Posts WHERE Id = @Id", CommandType.Text)
                    .DefineResults<dynamic>()
                    .Realize()
                    .Execute(this._db, new { Id = this.i }).First();
        }

        [Benchmark(Description = "Mapping Static")]
        public Post MappingStatic()
        {
            this.Step();
            return _cmd.Execute(this._db, new { Id = this.i }).First();
        }

        [Benchmark(Description = "Mapping Static (dynamic)")]
        public dynamic MappingStaticDynamic()
        {
            this.Step();
            return _cmdDynamic.Execute(this._db, new { Id = this.i }).First();
        }
    }
}