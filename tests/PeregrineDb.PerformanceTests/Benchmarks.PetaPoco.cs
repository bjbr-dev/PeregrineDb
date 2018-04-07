namespace PeregrineDb.PerformanceTests
{
    using System.Linq;
    using BenchmarkDotNet.Attributes;
    using PeregrineDb.PerformanceTests.PetaPoco;

    public class PetaPocoBenchmarks : BenchmarkBase
    {
        private Database _db, _dbFast;

        [GlobalSetup]
        public void Setup()
        {
            this.BaseSetup();
            this._db = new Database(ConnectionString, "System.Data.SqlClient");
            this._db.OpenSharedConnection();
            this._dbFast = new Database(ConnectionString, "System.Data.SqlClient");
            this._dbFast.OpenSharedConnection();
            this._dbFast.EnableAutoSelect = false;
            this._dbFast.EnableNamedParams = false;
            this._dbFast.ForceDateTimesToUtc = false;
        }

        [Benchmark(Description = "Fetch<Post>")]
        public Post Fetch()
        {
            this.Step();
            return this._db.Fetch<Post>("SELECT * from Posts where Id=@0", this.i).First();
        }

        [Benchmark(Description = "Fetch<Post> (Fast)")]
        public Post FetchFast()
        {
            this.Step();
            return this._dbFast.Fetch<Post>("SELECT * from Posts where Id=@0", this.i).First();
        }
    }
}