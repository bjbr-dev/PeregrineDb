namespace PeregrineDb
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using PeregrineDb.SqlCommands;

    public partial interface ISqlConnection
    {
        IReadOnlyList<T> RawQuery<T>(string sql, object parameters, CommandType commandType = CommandType.Text, int? commandTimeout = null);

        /// <summary>
        /// Executes a query, returning the data typed as <typeparamref name="T"/>.
        /// </summary>
        /// <typeparam name="T">The type of results to return.</typeparam>
        /// <param name="sql">The SQL to execute for the query.</param>
        /// <param name="commandTimeout">The command timeout (in seconds).</param>
        /// <returns>
        /// A sequence of data of the supplied type; if a basic type (int, string, etc) is queried then the data from the first column in assumed, otherwise an instance is
        /// created per row, and a direct column-name===member-name mapping is assumed (case insensitive).
        /// </returns>
        IReadOnlyList<T> Query<T>(FormattableString sql, int? commandTimeout = null);

        T RawQueryFirst<T>(string sql, object parameters, CommandType commandType = CommandType.Text, int? commandTimeout = null);

        T QueryFirst<T>(FormattableString sql, int? commandTimeout = null);

        T RawQueryFirstOrDefault<T>(string sql, object parameters, CommandType commandType = CommandType.Text, int? commandTimeout = null);

        T QueryFirstOrDefault<T>(FormattableString sql, int? commandTimeout = null);

        T RawQuerySingle<T>(string sql, object parameters, CommandType commandType = CommandType.Text, int? commandTimeout = null);

        T QuerySingle<T>(FormattableString sql, int? commandTimeout = null);

        T RawQuerySingleOrDefault<T>(string sql, object parameters, CommandType commandType = CommandType.Text, int? commandTimeout = null);

        T QuerySingleOrDefault<T>(FormattableString sql, int? commandTimeout = null);

        CommandResult RawExecuteMultiple<T>(string sql, IEnumerable<T> parameters, CommandType commandType = CommandType.Text, int? commandTimeout = null);

        CommandResult RawExecute<T>(string sql, T parameters, CommandType commandType = CommandType.Text, int? commandTimeout = null);

        CommandResult Execute(FormattableString sql, int? commandTimeout = null);

        T RawExecuteScalar<T>(string sql, object parameters, CommandType commandType = CommandType.Text, int? commandTimeout = null);

        T ExecuteScalar<T>(FormattableString sql, int? commandTimeout = null);
    }
}