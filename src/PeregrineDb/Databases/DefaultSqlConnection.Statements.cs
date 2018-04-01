namespace PeregrineDb.Databases
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using Dapper;
    using PeregrineDb.SqlCommands;

    public partial class DefaultSqlConnection
    {
        public IEnumerable<T> Query<T>(FormattableString sql, int? commandTimeout = null)
        {
            return this.connection.Query<T>(SqlString.ParameterizePlaceholders(sql), GetParameters(sql), this.transaction, true, commandTimeout);
        }

        public T QueryFirst<T>(FormattableString sql, int? commandTimeout = null)
        {
            return this.Query<T>(sql, commandTimeout).First();
        }

        public T QueryFirstOrDefault<T>(FormattableString sql, int? commandTimeout = null)
        {
            return this.Query<T>(sql, commandTimeout).FirstOrDefault();
        }

        public T QuerySingle<T>(FormattableString sql, int? commandTimeout = null)
        {
            return this.Query<T>(sql, commandTimeout).Single();
        }

        public T QuerySingleOrDefault<T>(FormattableString sql, int? commandTimeout = null)
        {
            return this.Query<T>(sql, commandTimeout).SingleOrDefault();
        }

        public CommandResult Execute(FormattableString sql, int? commandTimeout = null)
        {
            return new CommandResult(this.connection.Execute(SqlString.ParameterizePlaceholders(sql), GetParameters(sql), this.transaction, commandTimeout));
        }

        public T ExecuteScalar<T>(FormattableString sql, int? commandTimeout = null)
        {
            return this.connection.ExecuteScalar<T>(SqlString.ParameterizePlaceholders(sql), GetParameters(sql), this.transaction, commandTimeout);
        }

        private static DynamicParameters GetParameters(FormattableString sql)
        {
            var parameters = new DynamicParameters();
            var arguments = sql.GetArguments();
            for (var i = 0; i < arguments.Length; i++)
            {
                parameters.Add("@p" + i, arguments[i]);
            }

            return parameters;
        }
    }
}