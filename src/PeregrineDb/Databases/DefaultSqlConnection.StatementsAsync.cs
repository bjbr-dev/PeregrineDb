// <copyright file="DefaultSqlConnection.StatementsAsync.cs" company="Berkeleybross">
// Copyright (c) Berkeleybross. All rights reserved.
// </copyright>
namespace PeregrineDb.Databases
{
    using System.Collections.Generic;
    using System.Data;
    using System.Threading;
    using System.Threading.Tasks;
    using Dapper;
    using PeregrineDb.SqlCommands;

    public partial class DefaultSqlConnection
    {
        public async Task<IReadOnlyList<T>> QueryAsync<T>(string sql, object parameters, CommandType commandType, int? commandTimeout, CancellationToken cancellationToken)
        {
            var definition = new CommandDefinition(sql, parameters, this.Transaction, commandTimeout, commandType, CommandFlags.Buffered, cancellationToken);
            var result = await this.DbConnection.QueryAsync<T>(definition).ConfigureAwait(false);
            return (List<T>)result;
        }

        public Task<T> QueryFirstAsync<T>(string sql, object parameters, CommandType commandType, int? commandTimeout, CancellationToken cancellationToken)
        {
            var definition = new CommandDefinition(sql, parameters, this.Transaction, commandTimeout, commandType, CommandFlags.Buffered, cancellationToken);
            return this.DbConnection.QueryFirstAsync<T>(definition);
        }

        public Task<T> QueryFirstOrDefaultAsync<T>(string sql, object parameters, CommandType commandType, int? commandTimeout, CancellationToken cancellationToken)
        {
            var definition = new CommandDefinition(sql, parameters, this.Transaction, commandTimeout, commandType, CommandFlags.Buffered, cancellationToken);
            return this.DbConnection.QueryFirstOrDefaultAsync<T>(definition);
        }

        public Task<T> QuerySingleAsync<T>(string sql, object parameters, CommandType commandType, int? commandTimeout, CancellationToken cancellationToken)
        {
            var definition = new CommandDefinition(sql, parameters, this.Transaction, commandTimeout, commandType, CommandFlags.Buffered, cancellationToken);
            return this.DbConnection.QuerySingleAsync<T>(definition);
        }

        public Task<T> QuerySingleOrDefaultAsync<T>(string sql, object parameters, CommandType commandType, int? commandTimeout, CancellationToken cancellationToken)
        {
            var definition = new CommandDefinition(sql, parameters, this.Transaction, commandTimeout, commandType, CommandFlags.Buffered, cancellationToken);
            return this.DbConnection.QuerySingleOrDefaultAsync<T>(definition);
        }

        public async Task<CommandResult> ExecuteMultipleAsync<T>(string sql, IEnumerable<T> parameters, CommandType commandType = CommandType.Text, int? commandTimeout = null, CancellationToken cancellationToken = default)
        {
            var definition = new CommandDefinition(sql, parameters, this.Transaction, commandTimeout, commandType, CommandFlags.Buffered, cancellationToken);
            return new CommandResult(await this.DbConnection.ExecuteAsync(definition).ConfigureAwait(false));
        }

        public async Task<CommandResult> ExecuteAsync(string sql, object parameters, CommandType commandType, int? commandTimeout, CancellationToken cancellationToken)
        {
            var definition = new CommandDefinition(sql, parameters, this.Transaction, commandTimeout, commandType, CommandFlags.Buffered, cancellationToken);
            return new CommandResult(await this.DbConnection.ExecuteAsync(definition).ConfigureAwait(false));
        }

        public Task<T> ExecuteScalarAsync<T>(string sql, object parameters, CommandType commandType, int? commandTimeout, CancellationToken cancellationToken)
        {
            var definition = new CommandDefinition(sql, parameters, this.Transaction, commandTimeout, commandType, CommandFlags.Buffered, cancellationToken);
            return this.DbConnection.ExecuteScalarAsync<T>(definition);
        }
    }
}