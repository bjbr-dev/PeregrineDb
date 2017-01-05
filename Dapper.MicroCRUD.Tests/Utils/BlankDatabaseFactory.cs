// <copyright file="BlankDatabaseFactory.cs" company="Berkeleybross">
// Copyright (c) Berkeleybross. All rights reserved.
// </copyright>
namespace Dapper.MicroCRUD.Tests.Utils
{
    using System;
    using System.Collections.Generic;
    using System.Data.SqlClient;
    using System.Reflection;
    using DbUp;
    using DbUp.Builder;
    using Npgsql;

    internal class BlankDatabaseFactory
    {
        public static IEnumerable<string> PossibleDialects
            => new[]
                {
                    nameof(Dialect.SqlServer2012),
                    nameof(Dialect.PostgreSql)
                };

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
                ? @"Server=(local)\SQL2012SP1;Database=master;User ID=sa;Password=Password12!; Pooling=false"
                : @"Server=localhost; Integrated Security=true; Pooling=false";

            var databaseName = MakeRandomDatabaseName();
            var connectionStringBuilder = new SqlConnectionStringBuilder(serverConnectionString)
                {
                    InitialCatalog = databaseName,
                    MultipleActiveResultSets = false
                };

            var connectionString = connectionStringBuilder.ToString();

            EnsureDatabase.For.SqlDatabase(connectionString);
            CreateDatabase(DeployChanges.To.SqlDatabase(connectionString), "CreateSqlServer2012.sql");

            return new BlankDatabase(
                Dialect.SqlServer2012,
                new SqlConnection(connectionString),
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
            var connectionStringBuilder = new NpgsqlConnectionStringBuilder(serverConnectionString)
                {
                    Database = databaseName
                };

            var connectionString = connectionStringBuilder.ToString();

            EnsureDatabase.For.PostgresqlDatabase(connectionString);
            CreateDatabase(DeployChanges.To.PostgresqlDatabase(connectionString), "CreatePostgreSql.sql");

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

        private static void CreateDatabase(UpgradeEngineBuilder builder, string name)
        {
            var result = builder.WithScriptsEmbeddedInAssembly(
                                    Assembly.GetExecutingAssembly(),
                                    s => s == "Dapper.MicroCRUD.Tests.Scripts." + name)
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