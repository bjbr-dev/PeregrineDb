// <copyright file="BlankDatabaseFactory.cs" company="Berkeleybross">
// Copyright (c) Berkeleybross. All rights reserved.
// </copyright>
namespace Dapper.MicroCRUD.Tests.Utils
{
    using System;
    using System.Data.SqlClient;
    using System.IO;
    using System.Reflection;
    using Npgsql;

    internal class BlankDatabaseFactory
    {
        public static BlankDatabase MakeDatabase(string dialect)
        {
            var database = MakeDatabaseAux(dialect);
            database.Connection.Open();
            return database;
        }

        private static BlankDatabase MakeDatabaseAux(string dialect)
        {
            switch (dialect)
            {
                case nameof(Dialect.SqlServer2012):
                    return CreateSqlServer2012Database();
                case nameof(Dialect.PostgreSql):
                    return CreatePostgreSqlDatabase();
                default:
                    throw new InvalidOperationException();
            }
        }

        private static BlankDatabase CreateSqlServer2012Database()
        {
            var serverConnectionString = IsInAppVeyor()
                ? @"Server=(local)\SQL2014;Database=master;User ID=sa;Password=Password12!; Pooling=false"
                : @"Server=localhost; Integrated Security=true; Pooling=false";

            var databaseName = MakeRandomDatabaseName();
            using (var database = new SqlConnection(serverConnectionString))
            {
                database.Open();
                
                database.Execute("CREATE DATABASE " + databaseName);
            }

            var connectionString = new SqlConnectionStringBuilder(serverConnectionString)
                {
                    InitialCatalog = databaseName,
                    MultipleActiveResultSets = false
                };

            var sql = GetSql("CreateSqlServer2012.sql");
            using (var database = new SqlConnection(connectionString.ToString()))
            {
                database.Execute("CREATE SCHEMA Other;");
                database.Execute(sql);
            }

            return new BlankDatabase(
                Dialect.SqlServer2012,
                new SqlConnection(connectionString.ToString()),
                () =>
                {
                    using (var connection = new SqlConnection(serverConnectionString))
                    {
                        connection.Execute("USE MASTER; DROP DATABASE " + databaseName);
                    }
                });
        }

        private static BlankDatabase CreatePostgreSqlDatabase()
        {
            var serverConnectionString = IsInAppVeyor()
                ? "Server=localhost;Port=5432;User Id=postgres;Password=Password12!; Pooling=false;"
                : "Server=localhost;Port=5432;User Id=postgres;Password=postgres123; Pooling=false;";

            var databaseName = MakeRandomDatabaseName();
            using (var database = new NpgsqlConnection(serverConnectionString))
            {
                database.Execute("CREATE DATABASE " + databaseName);
            }

            var connectionStringBuilder = new NpgsqlConnectionStringBuilder(serverConnectionString)
                {
                    Database = databaseName
                };

            var connectionString = connectionStringBuilder.ToString();

            var sql = GetSql("CreatePostgreSql.sql");
            using (var database = new NpgsqlConnection(connectionString))
            {
                database.Execute(sql);
            }

            return new BlankDatabase(
                Dialect.PostgreSql,
                new NpgsqlConnection(connectionString),
                () =>
                {
                    using (var connection = new NpgsqlConnection(serverConnectionString))
                    {
                        connection.Execute("DROP DATABASE " + databaseName);
                    }
                });
        }

        private static string GetSql(string name)
        {
            string sql;
            using (var stream = typeof(BlankDatabaseFactory).GetTypeInfo().Assembly.GetManifestResourceStream("Dapper.MicroCRUD.Tests.Scripts." + name))
            {
                using (var reader = new StreamReader(stream))
                {
                    sql = reader.ReadToEnd();
                }
            }
            return sql;
        }

        private static string MakeRandomDatabaseName()
        {
            return "microcrudtests_" + Guid.NewGuid().ToString("N");
        }

        private static bool IsInAppVeyor()
        {
            var result = Environment.GetEnvironmentVariable("APPVEYOR");
            return string.Equals(result, "True", StringComparison.OrdinalIgnoreCase);
        }
    }
}