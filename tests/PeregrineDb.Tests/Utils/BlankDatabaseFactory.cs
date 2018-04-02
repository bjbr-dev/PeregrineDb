namespace PeregrineDb.Tests.Utils
{
    using System;
    using System.Collections.Concurrent;
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
        
        private static readonly ConcurrentDictionary<IDialect, ObjectPool<string>> ConnectionStringPool =
            new ConcurrentDictionary<IDialect, ObjectPool<string>>();
        
        private static readonly object Sync = new object();
        private static bool cleanedUp;

        public static IDatabase<IDbConnection> MakeDatabase(IDialect dialect)
        {
            switch (dialect)
            {
                case PostgreSqlDialect _:
                    return MakeDatabase(PeregrineConfig.Postgres);
                case SqlServer2012Dialect _:
                    return MakeDatabase(PeregrineConfig.SqlServer2012);
                default:
                    throw new NotSupportedException("Unknown dialect: " + dialect.GetType().Name);
            }
        }

        public static IDatabase<IDbConnection> MakeDatabase(PeregrineConfig config)
        {
            CleanUp();
            
            var dialect = config.Dialect;
            var dialectPool = ConnectionStringPool.GetOrAdd(dialect, d => new ObjectPool<string>(CreateDatabase));
            
            PooledInstance<string> pooledConnectionString = null;
            try
            {
                pooledConnectionString = dialectPool.Acquire();

                IDbConnection connection = null;
                try
                {
                    connection = OpenDatabase(pooledConnectionString.Item);
                    var pooledConnection = new PooledConnection<IDbConnection>(pooledConnectionString, connection);
                    var database = DefaultDatabase.From(pooledConnection, config);

                    DataWiper.ClearAllData(database);

                    return database;
                }
                catch
                {
                    connection?.Dispose();
                    throw;
                }
            }
            catch
            {
                pooledConnectionString?.Dispose();
                throw;
            }

            string CreateDatabase()
            {
                switch (dialect)
                {
                    case SqlServer2012Dialect _:
                        return CreateSqlServer2012Database();
                    case PostgreSqlDialect _:
                        return CreatePostgreSqlDatabase();
                    default:
                        throw new InvalidOperationException();
                }
            }

            IDbConnection OpenDatabase(string connectionString)
            {
                switch (dialect)
                {
                    case SqlServer2012Dialect _:
                    {
                        SqlConnection dbConnection = null;
                        try
                        {
                            dbConnection = new SqlConnection(connectionString);
                            dbConnection.Open();
                            return dbConnection;
                        }
                        catch
                        {
                            dbConnection?.Dispose();
                            throw;
                        }
                    }

                    case PostgreSqlDialect _:
                    {
                        NpgsqlConnection dbConnection = null;
                        try
                        {
                            dbConnection = new NpgsqlConnection(connectionString);
                            dbConnection.Open();
                            return dbConnection;
                        }
                        catch
                        {
                            dbConnection?.Dispose();
                            throw;
                        }
                    }

                    default:
                        throw new InvalidOperationException();
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
                        var databases = database.Query<string>($"SELECT name FROM sys.databases")
                                                .Where(s => s.StartsWith(DatabasePrefix));

                        foreach (var databaseName in databases)
                        {
                            if (!ProcessHelpers.IsRunning(GetProcessIdFromDatabaseName(databaseName)))
                            {
                                try
                                {
                                    database.Execute(new SqlString($@"USE master; DROP DATABASE {databaseName};"));
                                }
                                catch (SqlException ex)
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
                        var databases = database.Query<string>($"SELECT datname FROM pg_database")
                                                .Where(s => s.StartsWith(DatabasePrefix));

                        foreach (var databaseName in databases)
                        {
                            if (!ProcessHelpers.IsRunning(GetProcessIdFromDatabaseName(databaseName)))
                            {
                                try
                                {
                                    database.Execute(new SqlString($@"DROP DATABASE {databaseName};"));
                                }
                                catch (NpgsqlException ex)
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

        public static string CreateSqlServer2012Database()
        {
            var databaseName = MakeRandomDatabaseName();
            using (var con = new SqlConnection(TestSettings.SqlServerConnectionString))
            {
                con.Open();

                using (var database = DefaultDatabase.From(con, PeregrineConfig.SqlServer2012))
                {
                    database.Execute(new SqlString("CREATE DATABASE " + databaseName));
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
                    database.Execute($"CREATE SCHEMA Other;");
                    database.Execute(new SqlString(sql));
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
                    database.Execute(new SqlString("CREATE DATABASE " + databaseName));
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
                    database.Execute(new SqlString(GetSql("CreatePostgreSql.sql")));
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