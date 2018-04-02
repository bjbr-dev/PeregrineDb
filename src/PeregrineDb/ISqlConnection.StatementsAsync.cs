namespace PeregrineDb
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using PeregrineDb.SqlCommands;

    public partial interface ISqlConnection
    {
        Task<IEnumerable<T>> QueryAsync<T>(SqlCommand command, int? commandTimeout = null, CancellationToken cancellationToken = default);

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
        Task<IEnumerable<T>> QueryAsync<T>(FormattableString sql, int? commandTimeout = null, CancellationToken cancellationToken = default);

        Task<T> QueryFirstAsync<T>(SqlCommand command, int? commandTimeout = null, CancellationToken cancellationToken = default);

        Task<T> QueryFirstAsync<T>(FormattableString sql, int? commandTimeout = null, CancellationToken cancellationToken = default);

        Task<T> QueryFirstOrDefaultAsync<T>(SqlCommand command, int? commandTimeout = null, CancellationToken cancellationToken = default);

        Task<T> QueryFirstOrDefaultAsync<T>(FormattableString sql, int? commandTimeout = null, CancellationToken cancellationToken = default);

        Task<T> QuerySingleAsync<T>(SqlCommand command, int? commandTimeout = null, CancellationToken cancellationToken = default);

        Task<T> QuerySingleAsync<T>(FormattableString sql, int? commandTimeout = null, CancellationToken cancellationToken = default);

        Task<T> QuerySingleOrDefaultAsync<T>(SqlCommand command, int? commandTimeout = null, CancellationToken cancellationToken = default);

        Task<T> QuerySingleOrDefaultAsync<T>(FormattableString sql, int? commandTimeout = null, CancellationToken cancellationToken = default);

        Task<CommandResult> ExecuteAsync(SqlCommand command, int? commandTimeout = null, CancellationToken cancellationToken = default);

        Task<CommandResult> ExecuteAsync(FormattableString sql, int? commandTimeout = null, CancellationToken cancellationToken = default);

        Task<T> ExecuteScalarAsync<T>(SqlCommand command, int? commandTimeout = null, CancellationToken cancellationToken = default);

        Task<T> ExecuteScalarAsync<T>(FormattableString sql, int? commandTimeout = null, CancellationToken cancellationToken = default);
    }
}