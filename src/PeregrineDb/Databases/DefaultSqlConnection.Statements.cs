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
        public IReadOnlyList<T> Query<T>(in SqlCommand command, int? commandTimeout = null)
        {
            return this.connection.QueryImpl<T>(
                           new CommandDefinition(command.CommandText, command.Parameters, this.transaction, commandTimeout, command.CommandType), typeof(T))
                       .ToList();
        }

        public IReadOnlyList<T> Query<T>(FormattableString sql, int? commandTimeout = null)
        {
            var command = MakeCommand(sql);
            return this.Query<T>(in command, commandTimeout);
        }

        public T QueryFirst<T>(in SqlCommand command, int? commandTimeout = null)
        {
            var command1 = new CommandDefinition(command.CommandText, command.Parameters, this.transaction, commandTimeout, command.CommandType, CommandFlags.None);
            return SqlMapper.QueryRowImpl<T>(this.connection, SqlMapper.Row.First, ref command1, typeof(T));
        }

        public T QueryFirst<T>(FormattableString sql, int? commandTimeout = null)
        {
            var command = MakeCommand(sql);
            return this.QueryFirst<T>(in command, commandTimeout);
        }

        public T QueryFirstOrDefault<T>(in SqlCommand command, int? commandTimeout = null)
        {
            var command1 = new CommandDefinition(command.CommandText, command.Parameters, this.transaction, commandTimeout, command.CommandType, CommandFlags.None);
            return SqlMapper.QueryRowImpl<T>(this.connection, SqlMapper.Row.FirstOrDefault, ref command1, typeof(T));
        }

        public T QueryFirstOrDefault<T>(FormattableString sql, int? commandTimeout = null)
        {
            var command = MakeCommand(sql);
            return this.QueryFirstOrDefault<T>(in command, commandTimeout);
        }

        public T QuerySingle<T>(in SqlCommand command, int? commandTimeout = null)
        {
            var command1 = new CommandDefinition(command.CommandText, command.Parameters, this.transaction, commandTimeout, command.CommandType,
                CommandFlags.None);
            return SqlMapper.QueryRowImpl<T>(this.connection, SqlMapper.Row.Single, ref command1, typeof(T));
        }

        public T QuerySingle<T>(FormattableString sql, int? commandTimeout = null)
        {
            var command = MakeCommand(sql);
            return this.QuerySingle<T>(in command, commandTimeout);
        }

        public T QuerySingleOrDefault<T>(in SqlCommand command, int? commandTimeout = null)
        {
            return this.connection.QuerySingleOrDefault<T>(command.CommandText, command.Parameters, this.transaction, commandTimeout, command.CommandType);
        }

        public T QuerySingleOrDefault<T>(FormattableString sql, int? commandTimeout = null)
        {
            var command = MakeCommand(sql);
            return this.QuerySingleOrDefault<T>(in command, commandTimeout);
        }

        public CommandResult Execute(in SqlCommand command, int? commandTimeout = null)
        {
            string sql = command.CommandText;
            object param = command.Parameters;
            CommandType? commandType = command.CommandType;
            var command1 = new CommandDefinition(sql, param, this.transaction, commandTimeout, commandType, CommandFlags.Buffered);
            return new CommandResult(SqlMapper.ExecuteImpl(this.connection, ref command1));
        }

        public CommandResult Execute(FormattableString sql, int? commandTimeout = null)
        {
            var command = MakeCommand(sql);
            return this.Execute(in command, commandTimeout);
        }

        public T ExecuteScalar<T>(in SqlCommand command, int? commandTimeout = null)
        {
            string sql = command.CommandText;
            object param = command.Parameters;
            CommandType? commandType = command.CommandType;
            var command1 = new CommandDefinition(sql, param, this.transaction, commandTimeout, commandType, CommandFlags.Buffered);
            return SqlMapper.ExecuteScalarImpl<T>(this.connection, ref command1);
        }

        public T ExecuteScalar<T>(FormattableString sql, int? commandTimeout = null)
        {
            var command = MakeCommand(sql);
            return this.ExecuteScalar<T>(in command, commandTimeout);
        }

        private static SqlCommand MakeCommand(FormattableString sql)
        {
            var parameters = new DynamicParameters();
            var arguments = sql.GetArguments();
            for (var i = 0; i < arguments.Length; i++)
            {
                parameters.Add("@p" + i, arguments[i]);
            }

            return SqlCommandBuilder.MakeCommand(sql);
        }
    }
}