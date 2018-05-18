namespace PeregrineDb
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Threading;
    using System.Threading.Tasks;
    using PeregrineDb.SqlCommands;

    public partial interface ISqlConnection
    {
        Task<IReadOnlyList<T>> RawQueryAsync<T>(string sql, object parameters, CommandType commandType = CommandType.Text, int? commandTimeout = null, CancellationToken cancellationToken = default);

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
        Task<IReadOnlyList<T>> QueryAsync<T>(FormattableString sql, int? commandTimeout = null, CancellationToken cancellationToken = default);

        Task<T> RawQueryFirstAsync<T>(string sql, object parameters, CommandType commandType = CommandType.Text, int? commandTimeout = null, CancellationToken cancellationToken = default);

        Task<T> QueryFirstAsync<T>(FormattableString sql, int? commandTimeout = null, CancellationToken cancellationToken = default);

        Task<T> RawQueryFirstOrDefaultAsync<T>(string sql, object parameters, CommandType commandType = CommandType.Text, int? commandTimeout = null, CancellationToken cancellationToken = default);

        Task<T> QueryFirstOrDefaultAsync<T>(FormattableString sql, int? commandTimeout = null, CancellationToken cancellationToken = default);

        Task<T> RawQuerySingleAsync<T>(string sql, object parameters, CommandType commandType = CommandType.Text, int? commandTimeout = null, CancellationToken cancellationToken = default);

        Task<T> QuerySingleAsync<T>(FormattableString sql, int? commandTimeout = null, CancellationToken cancellationToken = default);

        Task<T> RawQuerySingleOrDefaultAsync<T>(string sql, object parameters, CommandType commandType = CommandType.Text, int? commandTimeout = null, CancellationToken cancellationToken = default);

        Task<T> QuerySingleOrDefaultAsync<T>(FormattableString sql, int? commandTimeout = null, CancellationToken cancellationToken = default);

        Task<CommandResult> RawExecuteMultipleAsync<T>(string sql, IEnumerable<T> parameters, CommandType commandType = CommandType.Text, int? commandTimeout = null, CancellationToken cancellationToken = default);
        
        Task<CommandResult> RawExecuteAsync<T>(string sql, T parameters, CommandType commandType = CommandType.Text, int? commandTimeout = null, CancellationToken cancellationToken = default);

        Task<CommandResult> ExecuteAsync(FormattableString sql, int? commandTimeout = null, CancellationToken cancellationToken = default);

        Task<T> RawExecuteScalarAsync<T>(string sql, object parameters, CommandType commandType = CommandType.Text, int? commandTimeout = null, CancellationToken cancellationToken = default);

        Task<T> ExecuteScalarAsync<T>(FormattableString sql, int? commandTimeout = null, CancellationToken cancellationToken = default);
    }
}