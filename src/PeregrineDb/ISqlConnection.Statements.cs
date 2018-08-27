// <copyright file="ISqlConnection.Statements.cs" company="Berkeleybross">
// Copyright (c) Berkeleybross. All rights reserved.
// </copyright>

namespace PeregrineDb
{
    using System.Collections.Generic;
    using System.Data;
    using PeregrineDb.SqlCommands;

    public partial interface ISqlConnection
    {
        IReadOnlyList<T> Query<T>(string sql, object parameters = null, CommandType commandType = CommandType.Text, int? commandTimeout = null);

        /// <summary>
        /// Executes a query, returning the data typed as <typeparamref name="T"/>.
        /// </summary>
        /// <typeparam name="T">The type of results to return.</typeparam>
        /// <param name="sql">The SQL to execute for the query.</param>
        /// <param name="parameters"></param>
        /// <param name="commandType"></param>
        /// <param name="commandTimeout">The command timeout (in seconds).</param>
        /// <returns>
        /// A sequence of data of the supplied type; if a basic type (int, string, etc) is queried then the data from the first column in assumed, otherwise an instance is
        /// created per row, and a direct column-name===member-name mapping is assumed (case insensitive).
        /// </returns>
        T QueryFirst<T>(string sql, object parameters = null, CommandType commandType = CommandType.Text, int? commandTimeout = null);

        T QueryFirstOrDefault<T>(string sql, object parameters = null, CommandType commandType = CommandType.Text, int? commandTimeout = null);

        T QuerySingle<T>(string sql, object parameters = null, CommandType commandType = CommandType.Text, int? commandTimeout = null);

        T QuerySingleOrDefault<T>(string sql, object parameters = null, CommandType commandType = CommandType.Text, int? commandTimeout = null);

        CommandResult ExecuteMultiple<T>(string sql, IEnumerable<T> parameters, CommandType commandType = CommandType.Text, int? commandTimeout = null);

        CommandResult Execute(string sql, object parameters = null, CommandType commandType = CommandType.Text, int? commandTimeout = null);

        T ExecuteScalar<T>(string sql, object parameters = null, CommandType commandType = CommandType.Text, int? commandTimeout = null);
    }
}