namespace PeregrineDb.Tests.Utils
{
    using System;
    using System.Collections.Concurrent;
    using System.Data.SqlClient;
    using System.IO;
    using System.Linq;
    using System.Reflection;
    using Dapper;
    using Npgsql;
    using PeregrineDb;
    using PeregrineDb.Databases;
    using PeregrineDb.Dialects;
    using PeregrineDb.Dialects.Postgres;
    using PeregrineDb.Testing;
    using PeregrineDb.Tests.Utils.Pooling;

    internal class BlankDatabaseFactory
    {
        private const string DatabasePrefix = "peregrinetests_";
        
        private static readonly ConcurrentDictionary<IDialect, ObjectPool<string>> ConnectionStringPool =
            new ConcurrentDictionary<IDialect, ObjectPool<string>>();
        
        private static readonly object Sync = new object();
        private static bool cleanedUp;

        public static PooledInstance<IDatabase> MakeDatabase(IDialect dialect)
        {
            return MakeDatabase(PeregrineConfig.SqlServer2012.WithDialect(dialect));
        }

        public static PooledInstance<IDatabase> MakeDatabase(PeregrineConfig config)
        {
            CleanUp();
            
            var dialect = config.Dialect;
            var dialectPool = ConnectionStringPool.GetOrAdd(dialect, d => new ObjectPool<string>(CreateDatabase));
            
            PooledInstance<string> pooledConnectionString = null;
            try
            {
                pooledConnectionString = dialectPool.Acquire();

                IDatabase database = null;
                try
                {
                    database = OpenDatabase(pooledConnectionString.Item);
                    DataWiper.ClearAllData(database);

                    return new PooledInstance<IDatabase>(database, d =>
                    {
                        d.Item.Dispose();
                        pooledConnectionString.Dispose();
                    });
                }
                catch
                {
                    database?.Dispose();
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
                switch (dialect.Name)
                {
                    case nameof(Dialect.SqlServer2012):
                        return CreateSqlServer2012Database();
                    case nameof(Dialect.PostgreSql):
                        return CreatePostgreSqlDatabase();
                    default:
                        throw new InvalidOperationException();
                }
            }

            IDatabase OpenDatabase(string connectionString)
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
                            return new DefaultDatabase(dbConnection, config);
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
                            return new DefaultDatabase(dbConnection, config);
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
                    var databases = con.Query<string>("SELECT name FROM sys.databases")
                                       .Where(s => s.StartsWith(DatabasePrefix));
                    
                    foreach (var databaseName in databases)
                    {
                        if (!ProcessHelpers.IsRunning(GetProcessIdFromDatabaseName(databaseName)))
                        {
                            try
                            {
                                con.Execute($@"USE master; DROP DATABASE {databaseName};");
                            }
                            catch (SqlException)
                            {
                                // Ignore errors since multiple processes can try to clean up the same database - only one can win
                                // Ideally we'd use a mutex but doesnt seem necessary - if we fail to cleanup we'll try again next time (or the other process did for us!)
                            }
                        }
                    }
                }
                
                using (var con = new NpgsqlConnection(TestSettings.PostgresServerConnectionString))
                {
                    var databases = con.Query<string>("SELECT datname FROM pg_database")
                                       .Where(s => s.StartsWith(DatabasePrefix));
                    
                    foreach (var databaseName in databases)
                    {
                        if (!ProcessHelpers.IsRunning(GetProcessIdFromDatabaseName(databaseName)))
                        {
                            try
                            {
                                con.Execute($@"DROP DATABASE {databaseName};");
                            }
                            catch (NpgsqlException)
                            {
                                // Ignore errors since multiple processes can try to clean up the same database - only one can win
                                // Ideally we'd use a mutex but doesnt seem necessary - if we fail to cleanup we'll try again next time (or the other process did for us!)
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
            using (var database = new SqlConnection(TestSettings.SqlServerConnectionString))
            {
                database.Open();

                database.Execute("CREATE DATABASE " + databaseName);
            }

            var connectionString = new SqlConnectionStringBuilder(TestSettings.SqlServerConnectionString)
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

            return connectionString.ToString();
        }

        private static string CreatePostgreSqlDatabase()
        {
            var databaseName = MakeRandomDatabaseName();
            using (var database = new NpgsqlConnection(TestSettings.PostgresServerConnectionString))
            {
                database.Execute("CREATE DATABASE " + databaseName);
            }

            var connectionStringBuilder = new NpgsqlConnectionStringBuilder(TestSettings.PostgresServerConnectionString)
                {
                    Database = databaseName
                };

            var connectionString = connectionStringBuilder.ToString();
            using (var database = new NpgsqlConnection(connectionString))
            {
                database.Execute(GetSql("CreatePostgreSql.sql"));
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