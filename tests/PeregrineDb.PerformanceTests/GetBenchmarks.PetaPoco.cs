// <copyright file="GetBenchmarks.PetaPoco.cs" company="Berkeleybross">
// Copyright (c) Berkeleybross. All rights reserved.
// </copyright>

namespace PeregrineDb.PerformanceTests
{
    using System.Linq;
    using BenchmarkDotNet.Attributes;
    using PetaPoco;

    public class PetaPocoGetBenchmarks
        : GetBenchmarks
    {
        private Database db, dbFast;

        [GlobalSetup]
        public void Setup()
        {
            this.BaseSetup();
            this.db = new Database(ConnectionString, "System.Data.SqlClient");
            this.db.OpenSharedConnection();
            this.dbFast = new Database(ConnectionString, "System.Data.SqlClient");
            this.dbFast.OpenSharedConnection();
            this.dbFast.EnableAutoSelect = false;
            this.dbFast.EnableNamedParams = false;
        }

        [Benchmark(Description = "Fetch{T}")]
        public Post Fetch()
        {
            this.Step();
            return this.db.Fetch<Post>("SELECT * from Posts where Id=@0", this.i).First();
        }

        [Benchmark(Description = "Fetch{T} (Fast)")]
        public Post FetchFast()
        {
            this.Step();
            return this.dbFast.Fetch<Post>("SELECT * from Posts where Id=@0", this.i).First();
        }
    }
}