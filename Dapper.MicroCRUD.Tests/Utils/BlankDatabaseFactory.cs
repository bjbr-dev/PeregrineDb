// <copyright file="BlankDatabaseFactory.cs" company="Berkeleybross">
// Copyright (c) Berkeleybross. All rights reserved.
// </copyright>
namespace Dapper.MicroCRUD.Tests.Utils
{
    using System;
    using System.Data;
    using System.Data.SqlClient;
    using System.Reflection;
    using DbUp;
    using DbUp.Builder;
    using Npgsql;

    public class BlankDatabaseFactory
    {
        public static IDbConnection CreateSqlServer2012Database()
        {
            var serverConnectionString = IsInAppVeyor()
                ? @"Server=(local)\SQL2012SP1;Database=master;User ID=sa;Password=Password12!"
                : @"Server=localhost; Integrated Security=true; Pooling=false";
            var connectionStringBuilder = new SqlConnectionStringBuilder(serverConnectionString)
                {
                    InitialCatalog = MakeRandomDatabaseName(),
                    PersistSecurityInfo = true
                };

            var connectionString = connectionStringBuilder.ToString();

            EnsureDatabase.For.SqlDatabase(connectionString);
            CreateDatabase(DeployChanges.To.SqlDatabase(connectionString), "CreateSqlServer2012.sql");

            return new SqlConnection(connectionString);
        }

        public static void DropSqlServer2012Database(string connectionString)
        {
            var builder = new SqlConnectionStringBuilder(connectionString);
            var currentDatabase = builder.InitialCatalog;
            builder.InitialCatalog = string.Empty;

            using (var connection = new SqlConnection(builder.ToString()))
            {
                connection.Execute("USE MASTER; DROP DATABASE " + currentDatabase);
            }
        }

        public static IDbConnection CreatePostgreSqlDatabase()
        {
            var serverConnectionString = IsInAppVeyor()
                ? "Server=localhost;Port=5432;User Id=postgres;Password=Password12!; Pooling=false;"
                : "Server=localhost;Port=5432;User Id=postgres;Password=postgres123; Pooling=false;";
            var connectionStringBuilder = new NpgsqlConnectionStringBuilder(serverConnectionString)
                {
                    Database = MakeRandomDatabaseName(),
                    PersistSecurityInfo = true
                };

            var connectionString = connectionStringBuilder.ToString();

            EnsureDatabase.For.PostgresqlDatabase(connectionString);
            CreateDatabase(DeployChanges.To.PostgresqlDatabase(connectionString), "CreatePostgreSql.sql");

            return new NpgsqlConnection(connectionString);
        }

        public static void DropPostgresDatabase(string connectionString)
        {
            var builder = new NpgsqlConnectionStringBuilder(connectionString);
            var currentDatabase = builder.Database;
            builder.Database = null;

            using (var connection = new NpgsqlConnection(builder.ToString()))
            {
                connection.Execute("DROP DATABASE " + currentDatabase);
            }
        }

        private static void CreateDatabase(UpgradeEngineBuilder builder, string name)
        {
            var result = builder.WithScriptsEmbeddedInAssembly(
                                    Assembly.GetExecutingAssembly(),
                                    s => s == "Dapper.MicroCRUD.Tests.Scripts." + name)
                                .LogToConsole()
                                .Build()
                                .PerformUpgrade();

            if (!result.Successful)
            {
                throw new InvalidOperationException("Could not deploy scripts for " + name);
            }
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