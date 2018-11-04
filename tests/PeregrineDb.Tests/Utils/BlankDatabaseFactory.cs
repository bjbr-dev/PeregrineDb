namespace PeregrineDb.Tests.Utils
{
    using System;
    using System.Data;
    using System.Data.SqlClient;
    using System.IO;
    using System.Linq;
    using System.Reflection;
    using Npgsql;
    using PeregrineDb;
    using PeregrineDb.Databases;
    using PeregrineDb.Dialects;
    using PeregrineDb.Testing;
    using PeregrineDb.Tests.Utils.Pooling;

    internal class BlankDatabaseFactory
    {
        private const string DatabasePrefix = "peregrinetests_";

        private static readonly ObjectPool<string> SqlServer2012Pool = new ObjectPool<string>(CreateSqlServer2012Database);
        private static readonly ObjectPool<string> PostgresPool = new ObjectPool<string>(CreatePostgreSqlDatabase);

        private static readonly object Sync = new object();
        private static bool cleanedUp;

        public static IDatabase<IDbConnection> MakeDatabase(IDialect dialect)
        {
            CleanUp();

            switch (dialect)
            {
                case PostgreSqlDialect _:
                    return OpenBlankDatabase(PostgresPool, cs => new NpgsqlConnection(cs), PeregrineConfig.Postgres);
                case SqlServer2012Dialect _:
                    return OpenBlankDatabase(SqlServer2012Pool, cs => new SqlConnection(cs), PeregrineConfig.SqlServer2012);
                default:
                    throw new NotSupportedException("Unknown dialect: " + dialect.GetType().Name);
            }

            IDatabase<IDbConnection> OpenBlankDatabase(
                ObjectPool<string> pool,
                Func<string, IDbConnection> makeConnection,
                PeregrineConfig config)
            {
                var pooledConnectionString = pool.Acquire();
                try
                {
                    IDbConnection dbConnection = null;
                    try
                    {
                        dbConnection = makeConnection(pooledConnectionString.Item);
                        dbConnection.Open();

                        IDbConnection pooledConnection = new PooledConnection<IDbConnection>(pooledConnectionString, dbConnection);
                        var database = DefaultDatabase.From(pooledConnection, config);

                        DataWiper.ClearAllData(database);
                        return database;
                    }
                    catch
                    {
                        dbConnection?.Dispose();
                        throw;
                    }
                }
                catch
                {
                    pooledConnectionString?.Dispose();
                    throw;
                }
            }
        }

        private static void CleanUp()
        {
            lock (Sync)
            {
                if (cleanedUp)
                {
                    return;
                }

                using (var con = new SqlConnection(TestSettings.SqlServerConnectionString))
                {
                    con.Open();

                    using (var database = DefaultDatabase.From(con, PeregrineConfig.SqlServer2012))
                    {
                        var databases = database.Query<string>("SELECT name FROM sys.databases")
                                                .Where(s => s.StartsWith(DatabasePrefix));

                        foreach (var databaseName in databases)
                        {
                            if (!ProcessHelpers.IsRunning(GetProcessIdFromDatabaseName(databaseName)))
                            {
                                try
                                {
                                    database.Execute($@"USE master; DROP DATABASE {databaseName};");
                                }
                                catch (SqlException)
                                {
                                    // Ignore errors since multiple processes can try to clean up the same database - only one can win
                                    // Ideally we'd use a mutex but doesnt seem necessary - if we fail to cleanup we'll try again next time (or the other process did for us!)
                                }
                            }
                        }
                    }
                }

                using (var con = new NpgsqlConnection(TestSettings.PostgresServerConnectionString))
                {
                    con.Open();

                    using (var database = DefaultDatabase.From(con, PeregrineConfig.Postgres))
                    {
                        var databases = database.Query<string>("SELECT datname FROM pg_database")
                                                .Where(s => s.StartsWith(DatabasePrefix));

                        foreach (var databaseName in databases)
                        {
                            if (!ProcessHelpers.IsRunning(GetProcessIdFromDatabaseName(databaseName)))
                            {
                                try
                                {
                                    database.Execute($@"DROP DATABASE {databaseName};");
                                }
                                catch (NpgsqlException)
                                {
                                    // Ignore errors since multiple processes can try to clean up the same database - only one can win
                                    // Ideally we'd use a mutex but doesnt seem necessary - if we fail to cleanup we'll try again next time (or the other process did for us!)
                                }
                            }
                        }

                    }
                }

                cleanedUp = true;
            }
        }

        private static string CreateSqlServer2012Database()
        {
            var databaseName = MakeRandomDatabaseName();
            using (var con = new SqlConnection(TestSettings.SqlServerConnectionString))
            {
                con.Open();

                using (var database = DefaultDatabase.From(con, PeregrineConfig.SqlServer2012))
                {
                    database.Execute("CREATE DATABASE " + databaseName);
                }
            }

            var connectionString = new SqlConnectionStringBuilder(TestSettings.SqlServerConnectionString)
                {
                    InitialCatalog = databaseName,
                    MultipleActiveResultSets = false
                };

            var sql = GetSql("CreateSqlServer2012.sql");
            using (var con = new SqlConnection(connectionString.ToString()))
            {
                con.Open();

                using (var database = DefaultDatabase.From(con, PeregrineConfig.SqlServer2012))
                {
                    database.Execute("CREATE SCHEMA Other;");
                    database.Execute(sql);
                }
            }

            return connectionString.ToString();
        }

        private static string CreatePostgreSqlDatabase()
        {
            var databaseName = MakeRandomDatabaseName();
            using (var con = new NpgsqlConnection(TestSettings.PostgresServerConnectionString))
            {
                con.Open();

                using (var database = DefaultDatabase.From(con, PeregrineConfig.Postgres))
                {
                    database.Execute("CREATE DATABASE " + databaseName);
                }
            }

            var connectionStringBuilder = new NpgsqlConnectionStringBuilder(TestSettings.PostgresServerConnectionString)
                {
                    Database = databaseName
                };

            var connectionString = connectionStringBuilder.ToString();
            using (var con = new NpgsqlConnection(connectionString))
            {
                con.Open();
                using (var database = DefaultDatabase.From(con, PeregrineConfig.Postgres))
                {
                    database.Execute(GetSql("CreatePostgreSql.sql"));
                    con.ReloadTypes();
                }
            }

            return connectionString;
        }

        private static string GetSql(string name)
        {
            string sql;
            using (var stream = typeof(BlankDatabaseFactory).GetTypeInfo().Assembly.GetManifestResourceStream("PeregrineDb.Tests.Scripts." + name))
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
            return DatabasePrefix + Guid.NewGuid().ToString("N") + "_" + ProcessHelpers.CurrentProcessId;
        }

        private static int GetProcessIdFromDatabaseName(string databaseName)
        {
            return int.Parse(databaseName.Substring(databaseName.LastIndexOf("_", StringComparison.Ordinal) + 1));
        }
    }
}