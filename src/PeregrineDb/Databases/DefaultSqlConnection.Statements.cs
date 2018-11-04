// <copyright file="DefaultSqlConnection.Statements.cs" company="Berkeleybross">
// Copyright (c) Berkeleybross. All rights reserved.
// </copyright>

namespace PeregrineDb.Databases
{
    using System.Collections.Generic;
    using System.Data;
    using Dapper;
    using PeregrineDb.SqlCommands;

    public partial class DefaultSqlConnection
    {
        public IReadOnlyList<T> Query<T>(string sql, object parameters, CommandType commandType, int? commandTimeout)
        {
            return (List<T>)this.DbConnection.Query<T>(sql, parameters, this.Transaction, true, commandTimeout, commandType);
        }

        public T QueryFirst<T>(string sql, object parameters, CommandType commandType = CommandType.Text, int? commandTimeout = null)
        {
            return this.DbConnection.QueryFirst<T>(sql, parameters, this.Transaction, commandTimeout, commandType);
        }

        public T QueryFirstOrDefault<T>(string sql, object parameters, CommandType commandType = CommandType.Text, int? commandTimeout = null)
        {
            return this.DbConnection.QueryFirstOrDefault<T>(sql, parameters, this.Transaction, commandTimeout, commandType);
        }

        public T QuerySingle<T>(string sql, object parameters, CommandType commandType = CommandType.Text, int? commandTimeout = null)
        {
            return this.DbConnection.QuerySingle<T>(sql, parameters, this.Transaction, commandTimeout, commandType);
        }

        public T QuerySingleOrDefault<T>(string sql, object parameters, CommandType commandType = CommandType.Text, int? commandTimeout = null)
        {
            return this.DbConnection.QuerySingleOrDefault<T>(sql, parameters, this.Transaction, commandTimeout, commandType);
        }

        public CommandResult ExecuteMultiple<T>(string sql, IEnumerable<T> parameters, CommandType commandType = CommandType.Text, int? commandTimeout = null)
        {
            return new CommandResult(this.DbConnection.Execute(sql, parameters, this.Transaction, commandTimeout, commandType));
        }

        public CommandResult Execute(string sql, object parameters, CommandType commandType = CommandType.Text, int? commandTimeout = null)
        {
            return new CommandResult(this.DbConnection.Execute(sql, parameters, this.Transaction, commandTimeout, commandType));
        }

        public T ExecuteScalar<T>(string sql, object parameters, CommandType commandType = CommandType.Text, int? commandTimeout = null)
        {
            return this.DbConnection.ExecuteScalar<T>(sql, parameters, this.Transaction, commandTimeout, commandType);
        }
    }
}