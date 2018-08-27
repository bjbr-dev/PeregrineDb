// <copyright file="ISqlConnection.StatementsAsync.cs" company="Berkeleybross">
// Copyright (c) Berkeleybross. All rights reserved.
// </copyright>

namespace PeregrineDb
{
    using System.Collections.Generic;
    using System.Data;
    using System.Threading;
    using System.Threading.Tasks;
    using PeregrineDb.SqlCommands;

    public partial interface ISqlConnection
    {
        /// <summary>
        /// Executes a query, returning the data typed as <typeparamref name="T"/>.
        /// </summary>
        /// <typeparam name="T">The type of results to return.</typeparam>
        /// <param name="sql">The SQL to execute for the query.</param>
        /// <param name="parameters"></param>
        /// <param name="commandType"></param>
        /// <param name="commandTimeout">The command timeout (in seconds).</param>
        /// <param name="cancellationToken"></param>
        /// <returns>
        /// A sequence of data of the supplied type; if a basic type (int, string, etc) is queried then the data from the first column in assumed, otherwise an instance is
        /// created per row, and a direct column-name===member-name mapping is assumed (case insensitive).
        /// </returns>
        Task<IReadOnlyList<T>> QueryAsync<T>(string sql, object parameters = null, CommandType commandType = CommandType.Text, int? commandTimeout = null, CancellationToken cancellationToken = default);

        Task<T> QueryFirstAsync<T>(string sql, object parameters = null, CommandType commandType = CommandType.Text, int? commandTimeout = null, CancellationToken cancellationToken = default);

        Task<T> QueryFirstOrDefaultAsync<T>(string sql, object parameters = null, CommandType commandType = CommandType.Text, int? commandTimeout = null, CancellationToken cancellationToken = default);

        Task<T> QuerySingleAsync<T>(string sql, object parameters = null, CommandType commandType = CommandType.Text, int? commandTimeout = null, CancellationToken cancellationToken = default);

        Task<T> QuerySingleOrDefaultAsync<T>(string sql, object parameters = null, CommandType commandType = CommandType.Text, int? commandTimeout = null, CancellationToken cancellationToken = default);

        Task<CommandResult> ExecuteMultipleAsync<T>(string sql, IEnumerable<T> parameters, CommandType commandType = CommandType.Text, int? commandTimeout = null, CancellationToken cancellationToken = default);

        Task<CommandResult> ExecuteAsync(string sql, object parameters = null, CommandType commandType = CommandType.Text, int? commandTimeout = null, CancellationToken cancellationToken = default);

        Task<T> ExecuteScalarAsync<T>(string sql, object parameters = null, CommandType commandType = CommandType.Text, int? commandTimeout = null, CancellationToken cancellationToken = default);
    }
}