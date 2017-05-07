// ReSharper disable once CheckNamespace
namespace Dapper
{
    using System.Collections.Generic;
    using System.Data;

    public static class DapperConnectionExtensions
    {
        public static IEnumerable<T> Query<T>(
            this IDapperConnection connection,
            string sql,
            object param = null,
            bool buffered = true,
            int? commandTimeout = null,
            CommandType? commandType = null)
        {
            return connection.DbConnection.Query<T>(sql, param, connection.Transaction, buffered, commandTimeout, commandType);
        }

        public static T QueryFirst<T>(
            this IDapperConnection connection,
            string sql,
            object param = null,
            int? commandTimeout = null,
            CommandType? commandType = null)
        {
            return connection.DbConnection.QueryFirst<T>(sql, param, connection.Transaction, commandTimeout, commandType);
        }

        public static T QueryFirstOrDefault<T>(
            this IDapperConnection connection,
            string sql,
            object param = null,
            int? commandTimeout = null,
            CommandType? commandType = null)
        {
            return connection.DbConnection.QueryFirstOrDefault<T>(sql, param, connection.Transaction, commandTimeout, commandType);
        }

        public static T QuerySingle<T>(
            this IDapperConnection connection,
            string sql,
            object param = null,
            int? commandTimeout = null,
            CommandType? commandType = null)
        {
            return connection.DbConnection.QuerySingle<T>(sql, param, connection.Transaction, commandTimeout, commandType);
        }

        public static T QuerySingleOrDefault<T>(
            this IDapperConnection connection,
            string sql,
            object param = null,
            int? commandTimeout = null,
            CommandType? commandType = null)
        {
            return connection.DbConnection.QuerySingleOrDefault<T>(sql, param, connection.Transaction, commandTimeout, commandType);
        }

        public static int Execute(
            this IDapperConnection connection,
            string sql,
            object param = null,
            int? commandTimeout = null,
            CommandType? commandType = null)
        {
            return connection.DbConnection.Execute(sql, param, connection.Transaction, commandTimeout, commandType);
        }

        public static T ExecuteScalar<T>(
            this IDapperConnection connection,
            string sql,
            object param = null,
            int? commandTimeout = null,
            CommandType? commandType = null)
        {
            return connection.DbConnection.ExecuteScalar<T>(sql, param, connection.Transaction, commandTimeout, commandType);
        }
    }
}