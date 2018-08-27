// <copyright file="DefaultSqlConnection.StatementsAsync.cs" company="Berkeleybross">
// Copyright (c) Berkeleybross. All rights reserved.
// </copyright>

namespace PeregrineDb.Databases
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Data.Common;
    using System.Threading;
    using System.Threading.Tasks;
    using PeregrineDb.Databases.Mapper;
    using PeregrineDb.Mapping;
    using PeregrineDb.SqlCommands;

    /// <remarks>
    /// Originally copied from Dapper.Net (https://github.com/StackExchange/dapper-dot-net) under the apache 2 license (http://www.apache.org/licenses/LICENSE-2.0).
    /// The code has been significantly altered.
    /// </remarks>
    public partial class DefaultSqlConnection
    {
        public async Task<IReadOnlyList<T>> QueryAsync<T>(string sql, object parameters, CommandType commandType, int? commandTimeout, CancellationToken cancellationToken)
        {
            var effectiveType = typeof(T);
            var identity = new Identity(sql, commandType, this.connection, effectiveType, parameters?.GetType(), null);
            var info = SqlMapper.GetCacheInfo(identity, parameters, true);
            var behavior = MapperSettings.Instance.GetBehavior(CommandBehavior.SequentialAccess | CommandBehavior.SingleResult);

            using (var command = (DbCommand)this.MakeCommand(sql, parameters, commandType, commandTimeout, info.ParamReader))
            {
                using (var reader = await command.ExecuteReaderAsync(behavior, cancellationToken).ConfigureAwait(false))
                {
                    var deserialzer = reader.MakeDeserializer<T>(info, effectiveType, identity);

                    var buffer = new List<T>();
                    while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                    {
                        buffer.Add(deserialzer(reader));
                    }

                    while (await reader.NextResultAsync(cancellationToken).ConfigureAwait(false))
                    {
                        /* ignore subsequent result sets */
                    }

                    return buffer;
                }
            }
        }

        public async Task<T> QueryFirstAsync<T>(string sql, object parameters, CommandType commandType, int? commandTimeout, CancellationToken cancellationToken)
        {
            var effectiveType = typeof(T);
            var identity = new Identity(sql, commandType, this.connection, effectiveType, parameters?.GetType(), null);
            var info = SqlMapper.GetCacheInfo(identity, parameters, true);
            var behavior = MapperSettings.Instance.GetBehavior(CommandBehavior.SequentialAccess | CommandBehavior.SingleResult | CommandBehavior.SingleRow);

            using (var command = (DbCommand)this.MakeCommand(sql, parameters, commandType, commandTimeout, info.ParamReader))
            {
                using (var reader = await command.ExecuteReaderAsync(behavior, cancellationToken).ConfigureAwait(false))
                {
                    if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false) || reader.FieldCount == 0)
                    {
                        throw new InvalidOperationException("Sequence contains no elements");
                    }

                    var deserialzer = reader.MakeDeserializer<T>(info, effectiveType, identity);
                    var result = deserialzer(reader);

                    while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                    {
                        /* ignore subsequent rows */
                    }

                    while (await reader.NextResultAsync(cancellationToken).ConfigureAwait(false))
                    {
                        /* ignore subsequent result sets */
                    }

                    return result;
                }
            }
        }

        public async Task<T> QueryFirstOrDefaultAsync<T>(string sql, object parameters, CommandType commandType, int? commandTimeout, CancellationToken cancellationToken)
        {
            var effectiveType = typeof(T);
            var identity = new Identity(sql, commandType, this.connection, effectiveType, parameters?.GetType(), null);
            var info = SqlMapper.GetCacheInfo(identity, parameters, true);
            var behavior = MapperSettings.Instance.GetBehavior(CommandBehavior.SequentialAccess | CommandBehavior.SingleResult | CommandBehavior.SingleRow);

            using (var command = (DbCommand)this.MakeCommand(sql, parameters, commandType, commandTimeout, info.ParamReader))
            {
                using (var reader = await command.ExecuteReaderAsync(behavior, cancellationToken).ConfigureAwait(false))
                {
                    T result = default;
                    if (await reader.ReadAsync(cancellationToken).ConfigureAwait(false) && reader.FieldCount != 0)
                    {
                        var deserialzer = reader.MakeDeserializer<T>(info, effectiveType, identity);
                        result = deserialzer(reader);

                        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                        {
                            /* ignore subsequent rows */
                        }
                    }

                    while (await reader.NextResultAsync(cancellationToken).ConfigureAwait(false))
                    {
                        /* ignore subsequent result sets */
                    }

                    return result;
                }
            }
        }

        public async Task<T> QuerySingleAsync<T>(string sql, object parameters, CommandType commandType, int? commandTimeout, CancellationToken cancellationToken)
        {
            var effectiveType = typeof(T);
            var identity = new Identity(sql, commandType, this.connection, effectiveType, parameters?.GetType(), null);
            var info = SqlMapper.GetCacheInfo(identity, parameters, true);
            var behavior = MapperSettings.Instance.GetBehavior(CommandBehavior.SequentialAccess | CommandBehavior.SingleResult);

            using (var command = (DbCommand)this.MakeCommand(sql, parameters, commandType, commandTimeout, info.ParamReader))
            {
                using (var reader = await command.ExecuteReaderAsync(behavior, cancellationToken).ConfigureAwait(false))
                {
                    if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false) || reader.FieldCount == 0)
                    {
                        throw new InvalidOperationException("Sequence contains no elements");
                    }

                    var deserialzer = reader.MakeDeserializer<T>(info, effectiveType, identity);
                    var result = deserialzer(reader);

                    if (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                    {
                        throw new InvalidOperationException("Sequence contains more than one element");
                    }

                    while (await reader.NextResultAsync(cancellationToken).ConfigureAwait(false))
                    {
                        /* ignore subsequent result sets */
                    }

                    return result;
                }
            }
        }

        public async Task<T> QuerySingleOrDefaultAsync<T>(string sql, object parameters, CommandType commandType, int? commandTimeout, CancellationToken cancellationToken)
        {
            var effectiveType = typeof(T);
            var identity = new Identity(sql, commandType, this.connection, effectiveType, parameters?.GetType(), null);
            var info = SqlMapper.GetCacheInfo(identity, parameters, true);
            var behavior = MapperSettings.Instance.GetBehavior(CommandBehavior.SequentialAccess | CommandBehavior.SingleResult);

            using (var cmd = (DbCommand)this.MakeCommand(sql, parameters, commandType, commandTimeout, info.ParamReader))
            {
                using (var reader = await cmd.ExecuteReaderAsync(behavior, cancellationToken).ConfigureAwait(false))
                {
                    T result = default;
                    if (await reader.ReadAsync(cancellationToken).ConfigureAwait(false) && reader.FieldCount != 0)
                    {
                        var deserialzer = reader.MakeDeserializer<T>(info, effectiveType, identity);
                        result = deserialzer(reader);

                        if (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                        {
                            throw new InvalidOperationException("Sequence contains more than one element");
                        }
                    }

                    while (await reader.NextResultAsync(cancellationToken).ConfigureAwait(false))
                    {
                        /* ignore subsequent result sets */
                    }

                    return result;
                }
            }
        }

        public async Task<CommandResult> ExecuteMultipleAsync<T>(string sql, IEnumerable<T> parameters, CommandType commandType, int? commandTimeout, CancellationToken cancellationToken)
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

        public async Task<CommandResult> ExecuteAsync(string sql, object parameters, CommandType commandType, int? commandTimeout, CancellationToken cancellationToken)
        {
            var identity = new Identity(sql, commandType, this.connection, null, parameters?.GetType(), null);
            var info = SqlMapper.GetCacheInfo(identity, parameters, true);
            using (var command = (DbCommand)this.MakeCommand(sql, parameters, commandType, commandTimeout, info?.ParamReader))
            {
                return new CommandResult(await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false));
            }
        }

        public async Task<T> ExecuteScalarAsync<T>(string sql, object parameters, CommandType commandType, int? commandTimeout, CancellationToken cancellationToken)
        {
            Action<IDbCommand, object> paramReader = null;
            if (parameters != null)
            {
                var identity = new Identity(sql, commandType, this.connection, null, parameters.GetType(), null);
                paramReader = SqlMapper.GetCacheInfo(identity, parameters, true).ParamReader;
            }

            using (var command = (DbCommand)this.MakeCommand(sql, parameters, commandType, commandTimeout, paramReader))
            {
                var result = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
                return TypeMapper.Parse<T>(result);
            }
        }

        private IDbCommand MakeCommand(string sql, object parameters, CommandType commandType, int? commandTimeout, Action<IDbCommand, object> paramReader)
        {
            var command = this.connection.CreateCommand();
            command.CommandText = sql;
            command.CommandType = commandType;

            if (this.transaction != null)
            {
                command.Transaction = this.transaction;
            }

            if (commandTimeout.HasValue)
            {
                command.CommandTimeout = commandTimeout.Value;
            }

            paramReader?.Invoke(command, parameters);
            return command;
        }
    }
}