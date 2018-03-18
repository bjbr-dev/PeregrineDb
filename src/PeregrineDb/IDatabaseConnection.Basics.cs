namespace PeregrineDb
{
    using System.Collections.Generic;
    using System.Data;

    public partial interface IDatabaseConnection
    {
        IEnumerable<T> Query<T>(string sql, object param = null, bool buffered = true, int? commandTimeout = null, CommandType? commandType = null);

        T QueryFirst<T>(string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null);
        
        T QueryFirstOrDefault<T>(string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null);

        T QuerySingle<T>(string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null);
        
        T QuerySingleOrDefault<T>(string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null);
        
        int Execute(string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null);

        T ExecuteScalar<T>(string sql, object param = null, int? commandTimeout = null, CommandType? commandType = null);
    }
}