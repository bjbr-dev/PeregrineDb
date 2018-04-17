namespace PeregrineDb.Databases
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Threading;
    using System.Threading.Tasks;
    using PeregrineDb.Databases.Mapper;
    using PeregrineDb.SqlCommands;

    public partial class DefaultSqlConnection
    {
        public async Task<IReadOnlyList<T>> QueryAsync<T>(SqlCommand command, int? commandTimeout = null, CancellationToken cancellationToken = default)
        {
            var definition = new CommandDefinition(
                command.CommandText, command.Parameters, this.transaction, commandTimeout, command.CommandType,
                CommandFlags.Buffered, cancellationToken);
            var result = await this.connection.QueryAsync<T>(typeof(T), definition).ConfigureAwait(false);
            return (List<T>)result;
        }

        public Task<IReadOnlyList<T>> QueryAsync<T>(FormattableString sql, int? commandTimeout = null, CancellationToken cancellationToken = default)
        {
            var command = MakeCommand(sql);
            return this.QueryAsync<T>(command, commandTimeout, cancellationToken);
        }

        public Task<T> QueryFirstAsync<T>(SqlCommand command, int? commandTimeout = null, CancellationToken cancellationToken = default)
        {
            return this.connection.QueryRowAsync<T>(SqlMapper.Row.First, typeof(T),
                new CommandDefinition(command.CommandText, command.Parameters, this.transaction, commandTimeout, command.CommandType,
                    CommandFlags.None, cancellationToken));
        }

        public Task<T> QueryFirstAsync<T>(FormattableString sql, int? commandTimeout = null, CancellationToken cancellationToken = default)
        {
            var command = MakeCommand(sql);
            return this.QueryFirstAsync<T>(command, commandTimeout, cancellationToken);
        }

        public Task<T> QueryFirstOrDefaultAsync<T>(SqlCommand command, int? commandTimeout = null, CancellationToken cancellationToken = default)
        {
            return this.connection.QueryRowAsync<T>(SqlMapper.Row.FirstOrDefault, typeof(T),
                new CommandDefinition(command.CommandText, command.Parameters, this.transaction, commandTimeout, command.CommandType,
                    CommandFlags.None, cancellationToken));
        }

        public Task<T> QueryFirstOrDefaultAsync<T>(FormattableString sql, int? commandTimeout = null, CancellationToken cancellationToken = default)
        {
            var command = MakeCommand(sql);
            return this.QueryFirstOrDefaultAsync<T>(command, commandTimeout, cancellationToken);
        }

        public Task<T> QuerySingleAsync<T>(SqlCommand command, int? commandTimeout = null, CancellationToken cancellationToken = default)
        {
            string sql = command.CommandText;
            object param = command.Parameters;
            CommandType? commandType = command.CommandType;
            return this.connection.QueryRowAsync<T>(SqlMapper.Row.Single, typeof(T), new CommandDefinition(sql, param, this.transaction, commandTimeout, commandType, CommandFlags.None, cancellationToken));
        }

        public Task<T> QuerySingleAsync<T>(FormattableString sql, int? commandTimeout = null, CancellationToken cancellationToken = default)
        {
            var command = MakeCommand(sql);
            return this.QuerySingleAsync<T>(command, commandTimeout, cancellationToken);
        }

        public Task<T> QuerySingleOrDefaultAsync<T>(SqlCommand command, int? commandTimeout = null, CancellationToken cancellationToken = default)
        {
            string sql = command.CommandText;
            object param = command.Parameters;
            CommandType? commandType = command.CommandType;
            return this.connection.QueryRowAsync<T>(SqlMapper.Row.SingleOrDefault, typeof(T), new CommandDefinition(sql, param, this.transaction, commandTimeout, commandType, CommandFlags.None, cancellationToken));
        }

        public Task<T> QuerySingleOrDefaultAsync<T>(FormattableString sql, int? commandTimeout = null, CancellationToken cancellationToken = default)
        {
            var command = MakeCommand(sql);
            return this.QuerySingleOrDefaultAsync<T>(command, commandTimeout, cancellationToken);
        }

        public async Task<CommandResult> ExecuteAsync(SqlCommand command, int? commandTimeout = null, CancellationToken cancellationToken = default)
        {
            string sql = command.CommandText;
            object param = command.Parameters;
            CommandType? commandType = command.CommandType;
            var numRows = await this.connection.ExecuteAsync(new CommandDefinition(sql, param, this.transaction, commandTimeout, commandType, CommandFlags.Buffered, cancellationToken))
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
            return this.connection.ExecuteScalarAsync<T>(command.CommandText, command.Parameters, this.transaction, commandTimeout, command.CommandType);
        }

        public Task<T> ExecuteScalarAsync<T>(FormattableString sql, int? commandTimeout = null, CancellationToken cancellationToken = default)
        {
            var command = MakeCommand(sql);
            return this.ExecuteScalarAsync<T>(command, commandTimeout, cancellationToken);
        }
    }
}