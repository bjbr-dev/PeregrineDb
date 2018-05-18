namespace PeregrineDb.Databases
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Linq;
    using PeregrineDb.Databases.Mapper;
    using PeregrineDb.Mapping;
    using PeregrineDb.SqlCommands;

    /// <remarks>
    /// Originally copied from Dapper.Net (https://github.com/StackExchange/dapper-dot-net) under the apache 2 license (http://www.apache.org/licenses/LICENSE-2.0).
    /// The code has been significantly altered.
    /// </remarks>
    public partial class DefaultSqlConnection
    {
        public IReadOnlyList<T> RawQuery<T>(string sql, object parameters, CommandType commandType = CommandType.Text, int? commandTimeout = null)
        {
            return Iterator().ToList();

            IEnumerable<T> Iterator()
            {
                var effectiveType = typeof(T);
                var identity = new Identity(sql, commandType, this.connection, effectiveType, parameters?.GetType(), null);
                var info = SqlMapper.GetCacheInfo(identity, parameters, true);

                using (var cmd = this.MakeCommand(sql, parameters, commandType, commandTimeout, info.ParamReader))
                {
                    using (var reader = cmd.ExecuteReader(MapperSettings.Instance.GetBehavior(CommandBehavior.SequentialAccess | CommandBehavior.SingleResult)))
                    {
                        if (reader.FieldCount == 0)
                        {
                            yield break;
                        }

                        var deserializer = reader.MakeDeserializer<T>(info, effectiveType, identity);

                        while (reader.Read())
                        {
                            yield return deserializer(reader);
                        }

                        while (reader.NextResult())
                        {
                            /* ignore subsequent result sets */
                        }
                    }
                }
            }
        }

        public IReadOnlyList<T> Query<T>(FormattableString sql, int? commandTimeout = null)
        {
            var command = MakeCommand(sql);
            return this.RawQuery<T>(command.CommandText, command.Parameters, CommandType.Text, commandTimeout);
        }

        public T RawQueryFirst<T>(string sql, object parameters, CommandType commandType = CommandType.Text, int? commandTimeout = null)
        {
            var effectiveType = typeof(T);
            var identity = new Identity(sql, commandType, this.connection, effectiveType, parameters?.GetType(), null);
            var info = SqlMapper.GetCacheInfo(identity, parameters, true);
            var behavior = MapperSettings.Instance.GetBehavior(CommandBehavior.SequentialAccess | CommandBehavior.SingleResult | CommandBehavior.SingleRow);

            using (var cmd = this.MakeCommand(sql, parameters, commandType, commandTimeout, info.ParamReader))
            {
                using (var reader = cmd.ExecuteReader(behavior))
                {
                    if (!reader.Read() || reader.FieldCount == 0)
                    {
                        throw new InvalidOperationException("Sequence contains no elements");
                    }

                    var deserialzer = reader.MakeDeserializer<T>(info, effectiveType, identity);
                    var result = deserialzer(reader);

                    while (reader.Read())
                    {
                        /* ignore subsequent rows */
                    }

                    while (reader.NextResult())
                    {
                        /* ignore subsequent result sets */
                    }

                    return result;
                }
            }
        }

        public T QueryFirst<T>(FormattableString sql, int? commandTimeout = null)
        {
            var command = MakeCommand(sql);
            return this.RawQueryFirst<T>(command.CommandText, command.Parameters, CommandType.Text, commandTimeout);
        }

        public T RawQueryFirstOrDefault<T>(string sql, object parameters, CommandType commandType = CommandType.Text, int? commandTimeout = null)
        {
            var effectiveType = typeof(T);
            var identity = new Identity(sql, commandType, this.connection, effectiveType, parameters?.GetType(), null);
            var info = SqlMapper.GetCacheInfo(identity, parameters, true);
            var behavior = MapperSettings.Instance.GetBehavior(CommandBehavior.SequentialAccess | CommandBehavior.SingleResult | CommandBehavior.SingleRow);

            using (var cmd = this.MakeCommand(sql, parameters, commandType, commandTimeout, info.ParamReader))
            {
                using (var reader = cmd.ExecuteReader(behavior))
                {
                    T result = default;
                    if (reader.Read() && reader.FieldCount != 0)
                    {
                        var deserialzer = reader.MakeDeserializer<T>(info, effectiveType, identity);
                        result = deserialzer(reader);

                        while (reader.Read())
                        {
                            /* ignore subsequent rows */
                        }
                    }

                    while (reader.NextResult())
                    {
                        /* ignore subsequent result sets */
                    }

                    return result;
                }
            }
        }

        public T QueryFirstOrDefault<T>(FormattableString sql, int? commandTimeout = null)
        {
            var command = MakeCommand(sql);
            return this.RawQueryFirstOrDefault<T>(command.CommandText, command.Parameters, CommandType.Text, commandTimeout);
        }

        public T RawQuerySingle<T>(string sql, object parameters, CommandType commandType = CommandType.Text, int? commandTimeout = null)
        {
            var effectiveType = typeof(T);
            var identity = new Identity(sql, commandType, this.connection, effectiveType, parameters?.GetType(), null);
            var info = SqlMapper.GetCacheInfo(identity, parameters, true);
            var behavior = MapperSettings.Instance.GetBehavior(CommandBehavior.SequentialAccess | CommandBehavior.SingleResult);

            using (var cmd = this.MakeCommand(sql, parameters, commandType, commandTimeout, info.ParamReader))
            {
                using (var reader = cmd.ExecuteReader(behavior))
                {
                    if (!reader.Read() || reader.FieldCount == 0)
                    {
                        throw new InvalidOperationException("Sequence contains no elements");
                    }

                    var deserialzer = reader.MakeDeserializer<T>(info, effectiveType, identity);
                    var result = deserialzer(reader);

                    if (reader.Read())
                    {
                        throw new InvalidOperationException("Sequence contains more than one element");
                    }

                    return result;
                }
            }
        }

        public T QuerySingle<T>(FormattableString sql, int? commandTimeout = null)
        {
            var command = MakeCommand(sql);
            return this.RawQuerySingle<T>(command.CommandText, command.Parameters, CommandType.Text, commandTimeout);
        }

        public T RawQuerySingleOrDefault<T>(string sql, object parameters, CommandType commandType = CommandType.Text, int? commandTimeout = null)
        {
            var effectiveType = typeof(T);
            var identity = new Identity(sql, commandType, this.connection, effectiveType, parameters?.GetType(), null);
            var info = SqlMapper.GetCacheInfo(identity, parameters, true);
            var behavior = MapperSettings.Instance.GetBehavior(CommandBehavior.SequentialAccess | CommandBehavior.SingleResult);

            using (var cmd = this.MakeCommand(sql, parameters, commandType, commandTimeout, info.ParamReader))
            {
                using (var reader = cmd.ExecuteReader(behavior))
                {
                    T result = default;
                    if (reader.Read() && reader.FieldCount != 0)
                    {
                        var deserialzer = reader.MakeDeserializer<T>(info, effectiveType, identity);
                        result = deserialzer(reader);

                        if (reader.Read())
                        {
                            throw new InvalidOperationException("Sequence contains more than one element");
                        }
                    }

                    while (reader.NextResult())
                    {
                        /* ignore subsequent result sets */
                    }

                    return result;
                }
            }
        }

        public T QuerySingleOrDefault<T>(FormattableString sql, int? commandTimeout = null)
        {
            var command = MakeCommand(sql);
            return this.RawQuerySingleOrDefault<T>(command.CommandText, command.Parameters, CommandType.Text, commandTimeout);
        }

        public CommandResult RawExecuteMultiple<T>(
            string sql,
            IEnumerable<T> parameters,
            CommandType commandType = CommandType.Text,
            int? commandTimeout = null)
        {
            CacheInfo info = null;
            var isFirst = true;
            var total = 0;

            using (var cmd = this.MakeCommand(sql, parameters, commandType, commandTimeout, null))
            {
                foreach (var obj in parameters)
                {
                    if (isFirst)
                    {
                        isFirst = false;
                        var identity = new Identity(sql, cmd.CommandType, this.connection, null, obj.GetType(), null);
                        info = SqlMapper.GetCacheInfo(identity, obj, true);
                    }
                    else
                    {
                        cmd.CommandText = sql; // because we do magic replaces on "in" etc
                        cmd.Parameters.Clear(); // current code is Add-tastic
                    }

                    info.ParamReader(cmd, obj);
                    total += cmd.ExecuteNonQuery();
                }
            }

            return new CommandResult(total);
        }

        public CommandResult RawExecute<T>(string sql, T parameters, CommandType commandType = CommandType.Text, int? commandTimeout = null)
        {
            Action<IDbCommand, object> paramReader = null;
            if (parameters != null)
            {
                var identity = new Identity(sql, commandType, this.connection, null, parameters.GetType(), null);
                paramReader = SqlMapper.GetCacheInfo(identity, parameters, true).ParamReader;
            }

            using (var cmd = this.MakeCommand(sql, parameters, commandType, commandTimeout, paramReader))
            {
                return new CommandResult(cmd.ExecuteNonQuery());
            }
        }

        public CommandResult Execute(FormattableString sql, int? commandTimeout = null)
        {
            var command = MakeCommand(sql);
            return this.RawExecute(command.CommandText, command.Parameters, CommandType.Text, commandTimeout);
        }

        public T RawExecuteScalar<T>(string sql, object parameters, CommandType commandType = CommandType.Text, int? commandTimeout = null)
        {
            Action<IDbCommand, object> paramReader = null;
            if (parameters != null)
            {
                var identity = new Identity(sql, commandType, this.connection, null, parameters.GetType(), null);
                paramReader = SqlMapper.GetCacheInfo(identity, parameters, true).ParamReader;
            }

            using (var cmd = this.MakeCommand(sql, parameters, commandType, commandTimeout, paramReader))
            {
                return TypeMapper.Parse<T>(cmd.ExecuteScalar());
            }
        }

        public T ExecuteScalar<T>(FormattableString sql, int? commandTimeout = null)
        {
            var command = MakeCommand(sql);
            return this.RawExecuteScalar<T>(command.CommandText, command.Parameters, CommandType.Text, commandTimeout);
        }

        private static (string CommandText, Dictionary<string, object> Parameters) MakeCommand(FormattableString sql)
        {
            var commandParameters = new Dictionary<string, object>();

            var i = 0;
            var arguments = sql.GetArguments();
            foreach (var parameter in arguments)
            {
                commandParameters["p" + i++] = parameter;
            }

            return (SqlString.ParameterizePlaceholders(sql.Format, arguments.Length), commandParameters);
        }
    }
}