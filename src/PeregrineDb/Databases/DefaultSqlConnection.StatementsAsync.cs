namespace PeregrineDb.Databases
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using Dapper;
    using PeregrineDb.SqlCommands;

    public partial class DefaultSqlConnection
    {
        public Task<IEnumerable<T>> QueryAsync<T>(SqlCommand command, int? commandTimeout = null, CancellationToken cancellationToken = default)
        {
            return this.connection.QueryAsync<T>(command.Text, command.Parameters, this.transaction, commandTimeout, command.Type);
        }

        public Task<IEnumerable<T>> QueryAsync<T>(FormattableString sql, int? commandTimeout = null, CancellationToken cancellationToken = default)
        {
            var command = MakeCommand(sql);
            return this.QueryAsync<T>(command, commandTimeout, cancellationToken);
        }

        public Task<T> QueryFirstAsync<T>(SqlCommand command, int? commandTimeout = null, CancellationToken cancellationToken = default)
        {
            return this.connection.QueryFirstAsync<T>(command.Text, command.Parameters, this.transaction, commandTimeout, command.Type);
        }

        public Task<T> QueryFirstAsync<T>(FormattableString sql, int? commandTimeout = null, CancellationToken cancellationToken = default)
        {
            var command = MakeCommand(sql);
            return this.QueryFirstAsync<T>(command, commandTimeout, cancellationToken);
        }

        public Task<T> QueryFirstOrDefaultAsync<T>(SqlCommand command, int? commandTimeout = null, CancellationToken cancellationToken = default)
        {
            return this.connection.QueryFirstOrDefaultAsync<T>(command.Text, command.Parameters, this.transaction, commandTimeout, command.Type);
        }

        public Task<T> QueryFirstOrDefaultAsync<T>(FormattableString sql, int? commandTimeout = null, CancellationToken cancellationToken = default)
        {
            var command = MakeCommand(sql);
            return this.QueryFirstOrDefaultAsync<T>(command, commandTimeout, cancellationToken);
        }

        public Task<T> QuerySingleAsync<T>(SqlCommand command, int? commandTimeout = null, CancellationToken cancellationToken = default)
        {
            return this.connection.QuerySingleAsync<T>(command.Text, command.Parameters, this.transaction, commandTimeout, command.Type);
        }

        public Task<T> QuerySingleAsync<T>(FormattableString sql, int? commandTimeout = null, CancellationToken cancellationToken = default)
        {
            var command = MakeCommand(sql);
            return this.QuerySingleAsync<T>(command, commandTimeout, cancellationToken);
        }

        public Task<T> QuerySingleOrDefaultAsync<T>(SqlCommand command, int? commandTimeout = null, CancellationToken cancellationToken = default)
        {
            return this.connection.QuerySingleOrDefaultAsync<T>(command.Text, command.Parameters, this.transaction, commandTimeout, command.Type);
        }

        public Task<T> QuerySingleOrDefaultAsync<T>(FormattableString sql, int? commandTimeout = null, CancellationToken cancellationToken = default)
        {
            var command = MakeCommand(sql);
            return this.QuerySingleOrDefaultAsync<T>(command, commandTimeout, cancellationToken);
        }

        public async Task<CommandResult> ExecuteAsync(SqlCommand command, int? commandTimeout = null, CancellationToken cancellationToken = default)
        {
            var numRows = await this.connection.ExecuteAsync(command.Text, command.Parameters, this.transaction, commandTimeout, command.Type)
                                    .ConfigureAwait(false);
            return new CommandResult(numRows);
        }

        public Task<CommandResult> ExecuteAsync(FormattableString sql, int? commandTimeout = null, CancellationToken cancellationToken = default)
        {
            var command = MakeCommand(sql);
            return this.ExecuteAsync(command, commandTimeout, cancellationToken);
        }

        public Task<T> ExecuteScalarAsync<T>(SqlCommand command, int? commandTimeout = null, CancellationToken cancellationToken = default)
        {
            return this.connection.ExecuteScalarAsync<T>(command.Text, command.Parameters, this.transaction, commandTimeout, command.Type);
        }

        public Task<T> ExecuteScalarAsync<T>(FormattableString sql, int? commandTimeout = null, CancellationToken cancellationToken = default)
        {
            var command = MakeCommand(sql);
            return this.ExecuteScalarAsync<T>(command, commandTimeout, cancellationToken);
        }
    }
}