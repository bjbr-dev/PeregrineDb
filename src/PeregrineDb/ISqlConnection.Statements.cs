namespace PeregrineDb
{
    using System;
    using System.Collections.Generic;
    using PeregrineDb.SqlCommands;

    public partial interface ISqlConnection
    {
        IReadOnlyList<T> Query<T>(in SqlCommand command, int? commandTimeout = null);

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

        T QueryFirst<T>(in SqlCommand command, int? commandTimeout = null);

        T QueryFirst<T>(FormattableString sql, int? commandTimeout = null);

        T QueryFirstOrDefault<T>(in SqlCommand command, int? commandTimeout = null);

        T QueryFirstOrDefault<T>(FormattableString sql, int? commandTimeout = null);

        T QuerySingle<T>(in SqlCommand command, int? commandTimeout = null);

        T QuerySingle<T>(FormattableString sql, int? commandTimeout = null);

        T QuerySingleOrDefault<T>(in SqlCommand command, int? commandTimeout = null);

        T QuerySingleOrDefault<T>(FormattableString sql, int? commandTimeout = null);

        CommandResult Execute(in SqlCommand command, int? commandTimeout = null);

        CommandResult Execute(FormattableString sql, int? commandTimeout = null);

        T ExecuteScalar<T>(in SqlCommand command, int? commandTimeout = null);

        T ExecuteScalar<T>(FormattableString sql, int? commandTimeout = null);
    }
}