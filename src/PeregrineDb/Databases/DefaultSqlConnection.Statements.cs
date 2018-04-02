﻿namespace PeregrineDb.Databases
{
    using System;
    using System.Collections.Generic;
    using Dapper;
    using PeregrineDb.SqlCommands;

    public partial class DefaultSqlConnection
    {
        public IEnumerable<T> Query<T>(in SqlCommand command, int? commandTimeout = null)
        {
            return this.connection.Query<T>(command.Text, command.Parameters, this.transaction, true, commandTimeout, command.Type);
        }

        public IEnumerable<T> Query<T>(FormattableString sql, int? commandTimeout = null)
        {
            var command = MakeCommand(sql);
            return this.Query<T>(in command, commandTimeout);
        }

        public T QueryFirst<T>(in SqlCommand command, int? commandTimeout = null)
        {
            return this.connection.QueryFirst<T>(command.Text, command.Parameters, this.transaction, commandTimeout, command.Type);
        }

        public T QueryFirst<T>(FormattableString sql, int? commandTimeout = null)
        {
            var command = MakeCommand(sql);
            return this.QueryFirst<T>(in command, commandTimeout);
        }

        public T QueryFirstOrDefault<T>(in SqlCommand command, int? commandTimeout = null)
        {
            return this.connection.QueryFirstOrDefault<T>(command.Text, command.Parameters, this.transaction, commandTimeout, command.Type);
        }

        public T QueryFirstOrDefault<T>(FormattableString sql, int? commandTimeout = null)
        {
            var command = MakeCommand(sql);
            return this.QueryFirstOrDefault<T>(in command, commandTimeout);
        }

        public T QuerySingle<T>(in SqlCommand command, int? commandTimeout = null)
        {
            return this.connection.QuerySingle<T>(command.Text, command.Parameters, this.transaction, commandTimeout, command.Type);
        }

        public T QuerySingle<T>(FormattableString sql, int? commandTimeout = null)
        {
            var command = MakeCommand(sql);
            return this.QuerySingle<T>(in command, commandTimeout);
        }

        public T QuerySingleOrDefault<T>(in SqlCommand command, int? commandTimeout = null)
        {
            return this.connection.QuerySingleOrDefault<T>(command.Text, command.Parameters, this.transaction, commandTimeout, command.Type);
        }

        public T QuerySingleOrDefault<T>(FormattableString sql, int? commandTimeout = null)
        {
            var command = MakeCommand(sql);
            return this.QuerySingleOrDefault<T>(in command, commandTimeout);
        }

        public CommandResult Execute(in SqlCommand command, int? commandTimeout = null)
        {
            return new CommandResult(this.connection.Execute(command.Text, command.Parameters, this.transaction, commandTimeout, command.Type));
        }

        public CommandResult Execute(FormattableString sql, int? commandTimeout = null)
        {
            var command = MakeCommand(sql);
            return this.Execute(in command, commandTimeout);
        }

        public T ExecuteScalar<T>(in SqlCommand command, int? commandTimeout = null)
        {
            return this.connection.ExecuteScalar<T>(command.Text, command.Parameters, this.transaction, commandTimeout, command.Type);
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