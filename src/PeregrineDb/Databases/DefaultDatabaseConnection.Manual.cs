namespace PeregrineDb.Databases
{
    using System.Collections.Generic;
    using System.Data;
    using Dapper;

    public partial class DefaultDatabaseConnection
    {
        public IEnumerable<T> Query<T>(string sql, object param = null, bool buffered = true, int? commandTimeout = null, CommandType? commandType = null)
        {
            return this.DbConnection.Query<T>(sql, param, this.transaction, buffered, commandTimeout, commandType);
        }

        public T QueryFirst<T>(string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null)
        {
            return this.DbConnection.QueryFirst<T>(sql, param, this.transaction, commandTimeout, commandType);
        }


        public T QueryFirstOrDefault<T>(string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null)
        {
            return this.DbConnection.QueryFirstOrDefault<T>(sql, param, this.transaction, commandTimeout, commandType);
        }

        public T QuerySingle<T>(string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null)
        {
            return this.DbConnection.QuerySingle<T>(sql, param, this.transaction, commandTimeout, commandType);
        }

        public T QuerySingleOrDefault<T>(string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null)
        {
            return this.DbConnection.QuerySingleOrDefault<T>(sql, param, this.transaction, commandTimeout, commandType);
        }

        public int Execute(string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null)
        {
            return this.DbConnection.Execute(sql, param, this.transaction, commandTimeout, commandType);
        }

        public T ExecuteScalar<T>(string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null)
        {
            return this.DbConnection.ExecuteScalar<T>(sql, param, this.transaction, commandTimeout, commandType);
        }
    }
}