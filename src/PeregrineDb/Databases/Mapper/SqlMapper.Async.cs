namespace PeregrineDb.Databases.Mapper
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Data;
    using System.Data.Common;
    using System.Globalization;
    using System.Threading;
    using System.Threading.Tasks;
    using PeregrineDb.Mapping;

    internal static partial class SqlMapper
    {
        public static async Task<IReadOnlyList<T>> QueryAsync<T>(this IDbConnection cnn, Type effectiveType, CommandDefinition command)
        {
            var param = command.Parameters;
            var identity = new Identity(command.CommandText, command.CommandType, cnn, effectiveType, param?.GetType(), null);
            var info = GetCacheInfo(identity, param, true);
            var cancel = command.CancellationToken;
            using (var cmd = (DbCommand)command.SetupCommand(cnn, info.ParamReader))
            {
                DbDataReader reader = null;
                try
                {
                    var commandBehavior = MapperSettings.Instance.GetBehavior(CommandBehavior.SequentialAccess | CommandBehavior.SingleResult);
                    reader = await cmd.ExecuteReaderAsync(commandBehavior, cancel).ConfigureAwait(false);

                    var tuple = info.Deserializer;
                    var hash = GetColumnHash(reader);
                    if (tuple.Func == null || tuple.Hash != hash)
                    {
                        tuple = info.Deserializer = new DeserializerState(hash, TypeMapper.GetDeserializer(effectiveType, reader, 0, -1, false));
                        QueryCache.SetQueryCache(identity, info);
                    }

                    var func = tuple.Func;

                    var buffer = new List<T>();
                    var convertToType = Nullable.GetUnderlyingType(effectiveType) ?? effectiveType;
                    while (await reader.ReadAsync(cancel).ConfigureAwait(false))
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

                    while (await reader.NextResultAsync(cancel).ConfigureAwait(false))
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

        public static async Task<T> QueryRowAsync<T>(this IDbConnection cnn, Row row, Type effectiveType, CommandDefinition command)
        {
            var param = command.Parameters;
            var identity = new Identity(command.CommandText, command.CommandType, cnn, effectiveType, param?.GetType(), null);
            var info = GetCacheInfo(identity, param, true);
            var cancel = command.CancellationToken;
            using (var cmd = (DbCommand)command.SetupCommand(cnn, info.ParamReader))
            {
                DbDataReader reader = null;
                try
                {
                    reader = await cmd.ExecuteReaderAsync(MapperSettings.Instance.GetBehavior((row & Row.Single) != 0
                        ? CommandBehavior.SequentialAccess | CommandBehavior.SingleResult // need to allow multiple rows, to check fail condition
                        : CommandBehavior.SequentialAccess | CommandBehavior.SingleResult | CommandBehavior.SingleRow), cancel).ConfigureAwait(false);

                    T result = default;
                    if (await reader.ReadAsync(cancel).ConfigureAwait(false) && reader.FieldCount != 0)
                    {
                        var tuple = info.Deserializer;
                        var hash = GetColumnHash(reader);
                        if (tuple.Func == null || tuple.Hash != hash)
                        {
                            tuple = info.Deserializer = new DeserializerState(hash, TypeMapper.GetDeserializer(effectiveType, reader, 0, -1, false));
                            if (true) QueryCache.SetQueryCache(identity, info);
                        }

                        var func = tuple.Func;

                        var val = func(reader);
                        if (val == null || val is T)
                        {
                            result = (T)val;
                        }
                        else
                        {
                            var convertToType = Nullable.GetUnderlyingType(effectiveType) ?? effectiveType;
                            result = (T)Convert.ChangeType(val, convertToType, CultureInfo.InvariantCulture);
                        }
                        if ((row & Row.Single) != 0 && await reader.ReadAsync(cancel).ConfigureAwait(false)) ThrowMultipleRows(row);
                        while (await reader.ReadAsync(cancel).ConfigureAwait(false)) { /* ignore rows after the first */ }
                    }
                    else if ((row & Row.FirstOrDefault) == 0) // demanding a row, and don't have one
                    {
                        ThrowZeroRows(row);
                    }
                    while (await reader.NextResultAsync(cancel).ConfigureAwait(false)) { /* ignore result sets after the first */ }
                    return result;
                }
                finally
                {
                    using (reader) { /* dispose if non-null */ }
                }
            }
        }

        /// <summary>
        /// Execute parameterized SQL that selects a single value.
        /// </summary>
        /// <typeparam name="T">The type to return.</typeparam>
        /// <param name="cnn">The connection to execute on.</param>
        /// <param name="sql">The SQL to execute.</param>
        /// <param name="param">The parameters to use for this command.</param>
        /// <param name="transaction">The transaction to use for this command.</param>
        /// <param name="commandTimeout">Number of seconds before command execution timeout.</param>
        /// <param name="commandType">Is it a stored proc or a batch?</param>
        /// <param name="cancellationToken"></param>
        /// <returns>The first cell returned, as <typeparamref name="T"/>.</returns>
        public static async Task<T> ExecuteScalarAsync<T>(
            this IDbConnection cnn,
            string sql,
            object param = null,
            IDbTransaction transaction = null,
            int? commandTimeout = null,
            CommandType? commandType = null,
            CancellationToken cancellationToken  = default)
        {
            var command = new CommandDefinition(sql, param, transaction, commandTimeout, commandType);
            Action<IDbCommand, object> paramReader = null;
            if (param != null)
            {
                var identity = new Identity(sql, commandType, cnn, null, param.GetType(), null);
                paramReader = GetCacheInfo(identity, param, true).ParamReader;
            }

            DbCommand cmd = null;
            object result;
            try
            {
                cmd = (DbCommand)command.SetupCommand(cnn, paramReader);
                result = await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                cmd?.Dispose();
            }

            return TypeMapper.Parse<T>(result);
        }
    }
}
