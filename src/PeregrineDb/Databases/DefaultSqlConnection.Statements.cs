namespace PeregrineDb.Databases
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Linq;
    using PeregrineDb.Databases.Mapper;
    using PeregrineDb.SqlCommands;

    public partial class DefaultSqlConnection
    {
        public IReadOnlyList<T> RawQuery<T>(string sql, object parameters, CommandType commandType = CommandType.Text, int? commandTimeout = null)
        {
            var definition = new CommandDefinition(sql, parameters, this.transaction, commandTimeout, commandType);
            return this.connection.QueryImpl<T>(definition, typeof(T)).ToList();
        }

        public IReadOnlyList<T> Query<T>(FormattableString sql, int? commandTimeout = null)
        {
            var command = MakeCommand(sql);
            return this.RawQuery<T>(command.CommandText, command.Parameters, CommandType.Text, commandTimeout);
        }

        public T RawQueryFirst<T>(string sql, object parameters, CommandType commandType = CommandType.Text, int? commandTimeout = null)
        {
            var command1 = new CommandDefinition(sql, parameters, this.transaction, commandTimeout, commandType);
            return SqlMapper.QueryRowImpl<T>(this.connection, SqlMapper.Row.First, ref command1, typeof(T));
        }

        public T QueryFirst<T>(FormattableString sql, int? commandTimeout = null)
        {
            var command = MakeCommand(sql);
            return this.RawQueryFirst<T>(command.CommandText, command.Parameters, CommandType.Text, commandTimeout);
        }

        public T RawQueryFirstOrDefault<T>(string sql, object parameters, CommandType commandType = CommandType.Text, int? commandTimeout = null)
        {
            var command1 = new CommandDefinition(sql, parameters, this.transaction, commandTimeout, commandType);
            return SqlMapper.QueryRowImpl<T>(this.connection, SqlMapper.Row.FirstOrDefault, ref command1, typeof(T));
        }

        public T QueryFirstOrDefault<T>(FormattableString sql, int? commandTimeout = null)
        {
            var command = MakeCommand(sql);
            return this.RawQueryFirstOrDefault<T>(command.CommandText, command.Parameters, CommandType.Text, commandTimeout);
        }

        public T RawQuerySingle<T>(string sql, object parameters, CommandType commandType = CommandType.Text, int? commandTimeout = null)
        {
            var command1 = new CommandDefinition(sql, parameters, this.transaction, commandTimeout, commandType);
            return SqlMapper.QueryRowImpl<T>(this.connection, SqlMapper.Row.Single, ref command1, typeof(T));
        }

        public T QuerySingle<T>(FormattableString sql, int? commandTimeout = null)
        {
            var command = MakeCommand(sql);
            return this.RawQuerySingle<T>(command.CommandText, command.Parameters, CommandType.Text, commandTimeout);
        }

        public T RawQuerySingleOrDefault<T>(string sql, object parameters, CommandType commandType = CommandType.Text, int? commandTimeout = null)
        {
            var command1 = new CommandDefinition(sql, parameters, this.transaction, commandTimeout, commandType);
            return SqlMapper.QueryRowImpl<T>(this.connection, SqlMapper.Row.SingleOrDefault, ref command1, typeof(T));
        }

        public T QuerySingleOrDefault<T>(FormattableString sql, int? commandTimeout = null)
        {
            var command = MakeCommand(sql);
            return this.RawQuerySingleOrDefault<T>(command.CommandText, command.Parameters, CommandType.Text, commandTimeout);
        }

        public CommandResult RawExecuteMultiple<T>(string sql, IEnumerable<T> parameters, CommandType commandType = CommandType.Text, int? commandTimeout = null)
        {
            var command1 = new CommandDefinition(sql, parameters, this.transaction, commandTimeout, commandType);

            var param = parameters;
            var multiExec = SqlMapper.GetMultiExec(param);
            Identity identity;
            CacheInfo info = null;
            if (multiExec != null)
            {
                var isFirst = true;
                var total = 0;

                using (var cmd = command1.SetupCommand(this.connection, null))
                {
                    string masterSql = null;
                    foreach (var obj in multiExec)
                    {
                        if (isFirst)
                        {
                            masterSql = cmd.CommandText;
                            isFirst = false;
                            identity = new Identity(sql, cmd.CommandType, this.connection, null, obj.GetType(), null);
                            info = SqlMapper.GetCacheInfo(identity, obj, true);
                        }
                        else
                        {
                            cmd.CommandText = masterSql; // because we do magic replaces on "in" etc
                            cmd.Parameters.Clear(); // current code is Add-tastic
                        }

                        info.ParamReader(cmd, obj);
                        total += cmd.ExecuteNonQuery();
                    }
                }

                return new CommandResult(total);
            }

            // nice and simple
            if (param != null)
            {
                identity = new Identity(sql, commandType, this.connection, null, param.GetType(), null);
                info = SqlMapper.GetCacheInfo(identity, param, true);
            }

            using (var cmd = command1.SetupCommand(this.connection, param == null ? null : info.ParamReader))
            {
                return new CommandResult(cmd.ExecuteNonQuery());
            }
        }

        public CommandResult RawExecute<T>(string sql, T parameters, CommandType commandType = CommandType.Text, int? commandTimeout = null)
        {
            var command = new CommandDefinition(sql, parameters, this.transaction, commandTimeout, commandType);

            CacheInfo info = null;

            // nice and simple
            if (parameters != null)
            {
                var identity = new Identity(sql, commandType, this.connection, null, parameters.GetType(), null);
                info = SqlMapper.GetCacheInfo(identity, parameters, true);
            }

            using (var cmd = command.SetupCommand(this.connection, parameters == null ? null : info.ParamReader))
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
            var command1 = new CommandDefinition(sql, parameters, this.transaction, commandTimeout, commandType);
            return SqlMapper.ExecuteScalarImpl<T>(this.connection, ref command1);
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