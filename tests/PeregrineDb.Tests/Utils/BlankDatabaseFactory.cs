namespace PeregrineDb.Tests.Utils
{
    using System;
    using System.Collections.Concurrent;
    using System.Data.SqlClient;
    using System.IO;
    using System.Reflection;
    using Dapper;
    using Npgsql;
    using PeregrineDb;
    using PeregrineDb.Databases;
    using PeregrineDb.Dialects;
    using PeregrineDb.Tests.Utils.Pooling;

    internal class BlankDatabaseFactory
    {
        private static readonly ConcurrentDictionary<IDialect, ObjectPool<string>> ConnectionStringPool =
            new ConcurrentDictionary<IDialect, ObjectPool<string>>();

        public static PooledInstance<IDatabase> MakeDatabase(IDialect dialect)
        {
            return MakeDatabase(DefaultConfig.MakeNewConfig().WithDialect(dialect));
        }

        public static PooledInstance<IDatabase> MakeDatabase(PeregrineConfig config)
        {
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

        private static string CreateSqlServer2012Database()
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

            return connectionString.ToString();
        }

        private static string CreatePostgreSqlDatabase()
        {
            var serverConnectionString = IsInAppVeyor()
                ? "Server=localhost;Port=5432;User Id=postgres;Password=Password12!; Pooling=false;"
                : "Server=10.10.3.202;Port=5432;User Id=postgres;Password=postgres123; Pooling=false;";

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
            return "microcrudtests_" + Guid.NewGuid().ToString("N");
        }

        private static bool IsInAppVeyor()
        {
            var result = Environment.GetEnvironmentVariable("APPVEYOR");
            return string.Equals(result, "True", StringComparison.OrdinalIgnoreCase);
        }
    }
}