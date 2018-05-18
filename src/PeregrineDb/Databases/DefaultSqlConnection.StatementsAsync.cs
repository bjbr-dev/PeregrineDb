namespace PeregrineDb.Databases
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Data.Common;
    using System.Threading;
    using System.Threading.Tasks;
    using PeregrineDb.Databases.Mapper;
    using PeregrineDb.SqlCommands;

    public partial class DefaultSqlConnection
    {
        public async Task<IReadOnlyList<T>> RawQueryAsync<T>(string sql, object parameters, CommandType commandType = CommandType.Text, int? commandTimeout = null, CancellationToken cancellationToken = default)
        {
            var definition = new CommandDefinition(sql, parameters, this.transaction, commandTimeout, commandType, cancellationToken);
            return await this.connection.QueryAsync<T>(typeof(T), definition).ConfigureAwait(false);
        }

        public Task<IReadOnlyList<T>> QueryAsync<T>(FormattableString sql, int? commandTimeout = null, CancellationToken cancellationToken = default)
        {
            var command = MakeCommand(sql);
            return this.RawQueryAsync<T>(command.CommandText, command.Parameters, CommandType.Text, commandTimeout, cancellationToken);
        }

        public Task<T> RawQueryFirstAsync<T>(string sql, object parameters, CommandType commandType = CommandType.Text, int? commandTimeout = null, CancellationToken cancellationToken = default)
        {
            return this.connection.QueryRowAsync<T>(SqlMapper.Row.First, typeof(T),
                new CommandDefinition(sql, parameters, this.transaction, commandTimeout, commandType, cancellationToken));
        }

        public Task<T> QueryFirstAsync<T>(FormattableString sql, int? commandTimeout = null, CancellationToken cancellationToken = default)
        {
            var command = MakeCommand(sql);
            return this.RawQueryFirstAsync<T>(command.CommandText, command.Parameters, CommandType.Text, commandTimeout, cancellationToken);
        }

        public Task<T> RawQueryFirstOrDefaultAsync<T>(string sql, object parameters, CommandType commandType = CommandType.Text, int? commandTimeout = null, CancellationToken cancellationToken = default)
        {
            return this.connection.QueryRowAsync<T>(SqlMapper.Row.FirstOrDefault, typeof(T),
                new CommandDefinition(sql, parameters, this.transaction, commandTimeout, commandType, cancellationToken));
        }

        public Task<T> QueryFirstOrDefaultAsync<T>(FormattableString sql, int? commandTimeout = null, CancellationToken cancellationToken = default)
        {
            var command = MakeCommand(sql);
            return this.RawQueryFirstOrDefaultAsync<T>(command.CommandText, command.Parameters, CommandType.Text, commandTimeout, cancellationToken);
        }

        public Task<T> RawQuerySingleAsync<T>(string sql, object parameters, CommandType commandType = CommandType.Text, int? commandTimeout = null, CancellationToken cancellationToken = default)
        {
            return this.connection.QueryRowAsync<T>(SqlMapper.Row.Single, typeof(T), new CommandDefinition(sql, parameters, this.transaction, commandTimeout, commandType, cancellationToken));
        }

        public Task<T> QuerySingleAsync<T>(FormattableString sql, int? commandTimeout = null, CancellationToken cancellationToken = default)
        {
            var command = MakeCommand(sql);
            return this.RawQuerySingleAsync<T>(command.CommandText, command.Parameters, CommandType.Text, commandTimeout, cancellationToken);
        }

        public Task<T> RawQuerySingleOrDefaultAsync<T>(string sql, object parameters, CommandType commandType = CommandType.Text, int? commandTimeout = null, CancellationToken cancellationToken = default)
        {
            return this.connection.QueryRowAsync<T>(SqlMapper.Row.SingleOrDefault, typeof(T),
                new CommandDefinition(sql, parameters, this.transaction, commandTimeout, commandType, cancellationToken));
        }

        public Task<T> QuerySingleOrDefaultAsync<T>(FormattableString sql, int? commandTimeout = null, CancellationToken cancellationToken = default)
        {
            var command = MakeCommand(sql);
            return this.RawQuerySingleOrDefaultAsync<T>(command.CommandText, command.Parameters, CommandType.Text, commandTimeout, cancellationToken);
        }

        public async Task<CommandResult> RawExecuteMultipleAsync<T>(
            string sql,
            IEnumerable<T> parameters,
            CommandType commandType,
            int? commandTimeout,
            CancellationToken cancellationToken)
        {
            var isFirst = true;
            var total = 0;
            CacheInfo info = null;
            string masterSql = null;

            using (var command = (DbCommand)this.MakeCommand(sql, null, commandType, commandTimeout, null))
            {
                foreach (var obj in parameters)
                {
                    if (isFirst)
                    {
                        masterSql = command.CommandText;
                        isFirst = false;
                        var identity = new Identity(sql, commandType, this.connection, null, obj.GetType(), null);
                        info = SqlMapper.GetCacheInfo(identity, obj, true);
                    }
                    else
                    {
                        command.CommandText = masterSql; // because we do magic replaces on "in" etc
                        command.Parameters.Clear(); // current code is Add-tastic
                    }

                    info.ParamReader(command, obj);
                    total += await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                }
            }

            return new CommandResult(total);
        }

        public async Task<CommandResult> RawExecuteAsync<T>(
            string sql,
            T parameters,
            CommandType commandType = CommandType.Text,
            int? commandTimeout = null,
            CancellationToken cancellationToken = default)
        {
            var identity = new Identity(sql, commandType, this.connection, null, parameters?.GetType(), null);
            var info = SqlMapper.GetCacheInfo(identity, parameters, true);
            using (var command = (DbCommand)this.MakeCommand(sql, parameters, commandType, commandTimeout, info))
            {
                return new CommandResult(await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false));
            }
        }

        public Task<CommandResult> ExecuteAsync(FormattableString sql, int? commandTimeout = null, CancellationToken cancellationToken = default)
        {
            var command = MakeCommand(sql);
            return this.RawExecuteAsync(command.CommandText, command.Parameters, CommandType.Text, commandTimeout, cancellationToken);
        }

        public Task<T> RawExecuteScalarAsync<T>(string sql, object parameters, CommandType commandType = CommandType.Text, int? commandTimeout = null, CancellationToken cancellationToken = default)
        {
            return this.connection.ExecuteScalarAsync<T>(sql, parameters, this.transaction, commandTimeout, commandType, cancellationToken);
        }

        public Task<T> ExecuteScalarAsync<T>(FormattableString sql, int? commandTimeout = null, CancellationToken cancellationToken = default)
        {
            var command = MakeCommand(sql);
            return this.RawExecuteScalarAsync<T>(command.CommandText, command.Parameters, CommandType.Text, commandTimeout, cancellationToken);
        }

        private IDbCommand MakeCommand(string sql, object parameters, CommandType commandType, int? commandTimeout, CacheInfo info)
        {
            return this.connection.MakeCommand(this.transaction, sql, commandTimeout, commandType, parameters, info?.ParamReader);
        }
    }
}