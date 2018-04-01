namespace PeregrineDb.Databases
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Dapper;
    using PeregrineDb.SqlCommands;

    public partial class DefaultSqlConnection
    {
        public Task<IEnumerable<T>> QueryAsync<T>(FormattableString sql, int? commandTimeout = null, CancellationToken cancellationToken = default)
        {
            return this.connection.QueryAsync<T>(SqlString.ParameterizePlaceholders(sql), GetParameters(sql), this.transaction, commandTimeout);
        }

        public async Task<T> QueryFirstAsync<T>(FormattableString sql, int? commandTimeout = null, CancellationToken cancellationToken = default)
        {
            var result = await this.QueryAsync<T>(sql, commandTimeout, cancellationToken).ConfigureAwait(false);
            return result.First();
        }

        public async Task<T> QueryFirstOrDefaultAsync<T>(FormattableString sql, int? commandTimeout = null, CancellationToken cancellationToken = default)
        {
            var result = await this.QueryAsync<T>(sql, commandTimeout, cancellationToken).ConfigureAwait(false);
            return result.FirstOrDefault();
        }

        public async Task<T> QuerySingleAsync<T>(FormattableString sql, int? commandTimeout = null, CancellationToken cancellationToken = default)
        {
            var result = await this.QueryAsync<T>(sql, commandTimeout, cancellationToken).ConfigureAwait(false);
            return result.Single();
        }

        public async Task<T> QuerySingleOrDefaultAsync<T>(FormattableString sql, int? commandTimeout = null, CancellationToken cancellationToken = default)
        {
            var result = await this.QueryAsync<T>(sql, commandTimeout, cancellationToken).ConfigureAwait(false);
            return result.SingleOrDefault();
        }

        public async Task<CommandResult> ExecuteAsync(FormattableString sql, int? commandTimeout = null, CancellationToken cancellationToken = default)
        {
            var result = await this.connection.ExecuteAsync(SqlString.ParameterizePlaceholders(sql), GetParameters(sql), this.transaction, commandTimeout);
            return new CommandResult(result);
        }

        public Task<T> ExecuteScalarAsync<T>(FormattableString sql, int? commandTimeout = null, CancellationToken cancellationToken = default)
        {
            return this.connection.ExecuteScalarAsync<T>(SqlString.ParameterizePlaceholders(sql), GetParameters(sql), this.transaction, commandTimeout);
        }
    }
}