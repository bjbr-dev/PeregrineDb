namespace PeregrineDb.Databases
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Data.Common;
    using System.Globalization;
    using System.Threading;
    using System.Threading.Tasks;
    using PeregrineDb.Databases.Mapper;
    using PeregrineDb.Mapping;
    using PeregrineDb.SqlCommands;

    public partial class DefaultSqlConnection
    {
        public async Task<IReadOnlyList<T>> RawQueryAsync<T>(
            string sql,
            object parameters,
            CommandType commandType = CommandType.Text,
            int? commandTimeout = null,
            CancellationToken cancellationToken = default)
        {
            var effectiveType = typeof(T);
            var identity = new Identity(sql, commandType, this.connection, effectiveType, parameters?.GetType(), null);
            var info = SqlMapper.GetCacheInfo(identity, parameters, true);
            var behavior = MapperSettings.Instance.GetBehavior(CommandBehavior.SequentialAccess | CommandBehavior.SingleResult);

            using (var command = (DbCommand)this.MakeCommand(sql, parameters, commandType, commandTimeout, info.ParamReader))
            {
                DbDataReader reader = null;
                try
                {
                    reader = await command.ExecuteReaderAsync(behavior, cancellationToken).ConfigureAwait(false);

                    var tuple = info.Deserializer;
                    var hash = SqlMapper.GetColumnHash(reader);
                    if (tuple.Func == null || tuple.Hash != hash)
                    {
                        tuple = info.Deserializer = new DeserializerState(hash, TypeMapper.GetDeserializer(effectiveType, reader, 0, -1, false));
                        QueryCache.SetQueryCache(identity, info);
                    }

                    var func = tuple.Func;

                    var buffer = new List<T>();
                    var convertToType = Nullable.GetUnderlyingType(effectiveType) ?? effectiveType;
                    while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                    {
                        var val = func(reader);
                        if (val == null || val is T)
                        {
                            buffer.Add((T)val);
                        }
                        else
                        {
                            buffer.Add((T)Convert.ChangeType(val, convertToType, CultureInfo.InvariantCulture));
                        }
                    }

                    while (await reader.NextResultAsync(cancellationToken).ConfigureAwait(false))
                    {
                        /* ignore subsequent result sets */
                    }

                    return buffer;
                }
                finally
                {
                    using (reader)
                    {
                        /* dispose if non-null */
                    }
                }
            }
        }

        public Task<IReadOnlyList<T>> QueryAsync<T>(FormattableString sql, int? commandTimeout = null, CancellationToken cancellationToken = default)
        {
            var command = MakeCommand(sql);
            return this.RawQueryAsync<T>(command.CommandText, command.Parameters, CommandType.Text, commandTimeout, cancellationToken);
        }

        public async Task<T> RawQueryFirstAsync<T>(
            string sql,
            object parameters,
            CommandType commandType = CommandType.Text,
            int? commandTimeout = null,
            CancellationToken cancellationToken = default)
        {
            var effectiveType = typeof(T);
            var identity = new Identity(sql, commandType, this.connection, effectiveType, parameters?.GetType(), null);
            var info = SqlMapper.GetCacheInfo(identity, parameters, true);
            var behavior = MapperSettings.Instance.GetBehavior(CommandBehavior.SequentialAccess | CommandBehavior.SingleResult | CommandBehavior.SingleRow);

            using (var command = (DbCommand)this.MakeCommand(sql, parameters, commandType, commandTimeout, info.ParamReader))
            {
                using (var reader = await command.ExecuteReaderAsync(behavior, cancellationToken).ConfigureAwait(false))
                {
                    T result;
                    if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false) || reader.FieldCount == 0)
                    {
                        throw new InvalidOperationException("Expected at least one row, but found none");
                    }

                    var tuple = info.Deserializer;
                    var hash = SqlMapper.GetColumnHash(reader);
                    if (tuple.Func == null || tuple.Hash != hash)
                    {
                        tuple = info.Deserializer = new DeserializerState(hash, TypeMapper.GetDeserializer(effectiveType, reader, 0, -1, false));
                        QueryCache.SetQueryCache(identity, info);
                    }

                    var val = tuple.Func(reader);
                    if (val == null || val is T)
                    {
                        result = (T)val;
                    }
                    else
                    {
                        var convertToType = Nullable.GetUnderlyingType(effectiveType) ?? effectiveType;
                        result = (T)Convert.ChangeType(val, convertToType, CultureInfo.InvariantCulture);
                    }

                    while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                    {
                        /* ignore rows after the first */
                    }

                    while (await reader.NextResultAsync(cancellationToken).ConfigureAwait(false))
                    {
                        /* ignore result sets after the first */
                    }

                    return result;
                }
            }
        }

        public Task<T> QueryFirstAsync<T>(FormattableString sql, int? commandTimeout = null, CancellationToken cancellationToken = default)
        {
            var command = MakeCommand(sql);
            return this.RawQueryFirstAsync<T>(command.CommandText, command.Parameters, CommandType.Text, commandTimeout, cancellationToken);
        }

        public async Task<T> RawQueryFirstOrDefaultAsync<T>(
            string sql,
            object parameters,
            CommandType commandType = CommandType.Text,
            int? commandTimeout = null,
            CancellationToken cancellationToken = default)
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
                        var tuple = info.Deserializer;
                        var hash = SqlMapper.GetColumnHash(reader);
                        if (tuple.Func == null || tuple.Hash != hash)
                        {
                            tuple = info.Deserializer = new DeserializerState(hash, TypeMapper.GetDeserializer(effectiveType, reader, 0, -1, false));
                            QueryCache.SetQueryCache(identity, info);
                        }

                        var val = tuple.Func(reader);
                        if (val == null || val is T)
                        {
                            result = (T)val;
                        }
                        else
                        {
                            var convertToType = Nullable.GetUnderlyingType(effectiveType) ?? effectiveType;
                            result = (T)Convert.ChangeType(val, convertToType, CultureInfo.InvariantCulture);
                        }

                        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                        {
                            /* ignore rows after the first */
                        }
                    }

                    while (await reader.NextResultAsync(cancellationToken).ConfigureAwait(false))
                    {
                        /* ignore result sets after the first */
                    }

                    return result;
                }
            }
        }

        public Task<T> QueryFirstOrDefaultAsync<T>(FormattableString sql, int? commandTimeout = null, CancellationToken cancellationToken = default)
        {
            var command = MakeCommand(sql);
            return this.RawQueryFirstOrDefaultAsync<T>(command.CommandText, command.Parameters, CommandType.Text, commandTimeout, cancellationToken);
        }

        public async Task<T> RawQuerySingleAsync<T>(
            string sql,
            object parameters,
            CommandType commandType = CommandType.Text,
            int? commandTimeout = null,
            CancellationToken cancellationToken = default)
        {
            var effectiveType = typeof(T);
            var identity = new Identity(sql, commandType, this.connection, effectiveType, parameters?.GetType(), null);
            var info = SqlMapper.GetCacheInfo(identity, parameters, true);
            var behavior = MapperSettings.Instance.GetBehavior(CommandBehavior.SequentialAccess | CommandBehavior.SingleResult);

            using (var command = (DbCommand)this.MakeCommand(sql, parameters, commandType, commandTimeout, info.ParamReader))
            {
                using (var reader = await command.ExecuteReaderAsync(behavior, cancellationToken).ConfigureAwait(false))
                {
                    T result;
                    if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false) || reader.FieldCount == 0)
                    {
                        throw new InvalidOperationException("Expected a single row, but found none");
                    }

                    var tuple = info.Deserializer;
                    var hash = SqlMapper.GetColumnHash(reader);
                    if (tuple.Func == null || tuple.Hash != hash)
                    {
                        tuple = info.Deserializer = new DeserializerState(hash, TypeMapper.GetDeserializer(effectiveType, reader, 0, -1, false));
                        QueryCache.SetQueryCache(identity, info);
                    }

                    var val = tuple.Func(reader);
                    if (val == null || val is T)
                    {
                        result = (T)val;
                    }
                    else
                    {
                        var convertToType = Nullable.GetUnderlyingType(effectiveType) ?? effectiveType;
                        result = (T)Convert.ChangeType(val, convertToType, CultureInfo.InvariantCulture);
                    }

                    if (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                    {
                        throw new InvalidOperationException("Expected a single row, but found multiple");
                    }

                    while (await reader.NextResultAsync(cancellationToken).ConfigureAwait(false))
                    {
                        /* ignore result sets after the first */
                    }

                    return result;
                }
            }
        }

        public Task<T> QuerySingleAsync<T>(FormattableString sql, int? commandTimeout = null, CancellationToken cancellationToken = default)
        {
            var command = MakeCommand(sql);
            return this.RawQuerySingleAsync<T>(command.CommandText, command.Parameters, CommandType.Text, commandTimeout, cancellationToken);
        }

        public async Task<T> RawQuerySingleOrDefaultAsync<T>(
            string sql,
            object parameters,
            CommandType commandType = CommandType.Text,
            int? commandTimeout = null,
            CancellationToken cancellationToken = default)
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
                        var tuple = info.Deserializer;
                        var hash = SqlMapper.GetColumnHash(reader);
                        if (tuple.Func == null || tuple.Hash != hash)
                        {
                            tuple = info.Deserializer = new DeserializerState(hash, TypeMapper.GetDeserializer(effectiveType, reader, 0, -1, false));
                            QueryCache.SetQueryCache(identity, info);
                        }

                        var val = tuple.Func(reader);
                        if (val == null || val is T)
                        {
                            result = (T)val;
                        }
                        else
                        {
                            var convertToType = Nullable.GetUnderlyingType(effectiveType) ?? effectiveType;
                            result = (T)Convert.ChangeType(val, convertToType, CultureInfo.InvariantCulture);
                        }

                        if (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                        {
                            throw new InvalidOperationException("Expected a single row, but found multiple");
                        }
                    }

                    while (await reader.NextResultAsync(cancellationToken).ConfigureAwait(false))
                    {
                        /* ignore result sets after the first */
                    }

                    return result;
                }
            }
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
            using (var command = (DbCommand)this.MakeCommand(sql, parameters, commandType, commandTimeout, info?.ParamReader))
            {
                return new CommandResult(await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false));
            }
        }

        public Task<CommandResult> ExecuteAsync(FormattableString sql, int? commandTimeout = null, CancellationToken cancellationToken = default)
        {
            var command = MakeCommand(sql);
            return this.RawExecuteAsync(command.CommandText, command.Parameters, CommandType.Text, commandTimeout, cancellationToken);
        }

        public async Task<T> RawExecuteScalarAsync<T>(string sql, object parameters, CommandType commandType = CommandType.Text, int? commandTimeout = null, CancellationToken cancellationToken = default)
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

        public Task<T> ExecuteScalarAsync<T>(FormattableString sql, int? commandTimeout = null, CancellationToken cancellationToken = default)
        {
            var command = MakeCommand(sql);
            return this.RawExecuteScalarAsync<T>(command.CommandText, command.Parameters, CommandType.Text, commandTimeout, cancellationToken);
        }

        private IDbCommand MakeCommand(string sql, object parameters, CommandType commandType, int? commandTimeout, Action<IDbCommand, object> paramReader)
        {
            return this.connection.MakeCommand(this.transaction, sql, commandTimeout, commandType, parameters, paramReader);
        }
    }
}