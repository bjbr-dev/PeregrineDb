// <copyright file="GetBenchmarks.HandCoded.cs" company="Berkeleybross">
// Copyright (c) Berkeleybross. All rights reserved.
// </copyright>

namespace PeregrineDb.PerformanceTests
{
    using System;
    using System.Data;
    using System.Data.SqlClient;
    using BenchmarkDotNet.Attributes;

    public class HandCodedGetBenchmarks : GetBenchmarks
    {
        private SqlCommand postCommand;
        private SqlParameter idParam;
        private DataTable table;

        [GlobalSetup]
        public void Setup()
        {
            this.BaseSetup();
            this.postCommand = new SqlCommand
            {
                Connection = this.Connection,
                CommandText = @"select Id, [Text], [CreationDate], LastChangeDate, 
                Counter1,Counter2,Counter3,Counter4,Counter5,Counter6,Counter7,Counter8,Counter9 from Posts where Id = @Id"
            };
            this.idParam = this.postCommand.Parameters.Add("@Id", SqlDbType.Int);
            this.table = new DataTable
                {
                    Columns =
                        {
                            { "Id", typeof(int) },
                            { "Text", typeof(string) },
                            { "CreationDate", typeof(DateTime) },
                            { "LastChangeDate", typeof(DateTime) },
                            { "Counter1", typeof(int) },
                            { "Counter2", typeof(int) },
                            { "Counter3", typeof(int) },
                            { "Counter4", typeof(int) },
                            { "Counter5", typeof(int) },
                            { "Counter6", typeof(int) },
                            { "Counter7", typeof(int) },
                            { "Counter8", typeof(int) },
                            { "Counter9", typeof(int) },
                        }
                };
        }

        [Benchmark(Description = "SqlCommand", Baseline = true)]
        public Post SqlCommand()
        {
            this.Step();
            this.idParam.Value = this.i;

            using (var reader = this.postCommand.ExecuteReader())
            {
                reader.Read();
                return new Post
                    {
                        Id = reader.GetInt32(0),
                        Text = reader.GetNullableString(1),
                        CreationDate = reader.GetDateTime(2),
                        LastChangeDate = reader.GetDateTime(3),
                        Counter1 = reader.GetNullableValue<int>(4),
                        Counter2 = reader.GetNullableValue<int>(5),
                        Counter3 = reader.GetNullableValue<int>(6),
                        Counter4 = reader.GetNullableValue<int>(7),
                        Counter5 = reader.GetNullableValue<int>(8),
                        Counter6 = reader.GetNullableValue<int>(9),
                        Counter7 = reader.GetNullableValue<int>(10),
                        Counter8 = reader.GetNullableValue<int>(11),
                        Counter9 = reader.GetNullableValue<int>(12)
                    };
            }
        }

        [Benchmark(Description = "DataTable")]
        public dynamic DataTableDynamic()
        {
            this.Step();
            this.idParam.Value = this.i;
            var values = new object[13];
            using (var reader = this.postCommand.ExecuteReader())
            {
                reader.Read();
                reader.GetValues(values);
                this.table.Rows.Add(values);
                return this.table.Rows[this.table.Rows.Count - 1];
            }
        }
    }
}
