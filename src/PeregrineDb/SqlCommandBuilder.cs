// <copyright file="SqlCommandBuilder.cs" company="Berkeleybross">
// Copyright (c) Berkeleybross. All rights reserved.
// </copyright>

namespace PeregrineDb
{
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using PeregrineDb.Schema;

    public class SqlCommandBuilder
    {
        private readonly StringBuilder builder = new StringBuilder();

        public SqlCommandBuilder(string value)
        {
            this.builder.Append(value);
        }

        public SqlCommandBuilder Append(string value)
        {
            this.builder.Append(value);
            return this;
        }

        public SqlCommandBuilder Append(object value)
        {
            this.builder.Append(value);
            return this;
        }

        public SqlCommandBuilder AppendLine()
        {
            this.builder.AppendLine();
            return this;
        }

        public SqlCommandBuilder AppendLine(string value)
        {
            this.builder.AppendLine(value);
            return this;
        }

        public SqlCommandBuilder AppendFormat(string format, object arg0)
        {
            this.builder.AppendFormat(format, arg0);
            return this;
        }

        public SqlCommandBuilder AppendFormat(string format, object arg0, object arg1)
        {
            this.builder.AppendFormat(format, arg0, arg1);
            return this;
        }

        public SqlCommandBuilder AppendFormat(string format, object arg0, object arg1, object arg2)
        {
            this.builder.AppendFormat(format, arg0, arg1, arg2);
            return this;
        }

        public SqlCommandBuilder AppendFormat(string format, params object[] values)
        {
            this.builder.AppendFormat(format, values);
            return this;
        }

        /// <summary>
        /// Appends an arbitrary clause of SQL to the string. Adds a new line at the start if <paramref name="clause"/> is not empty.
        /// </summary>
        public SqlCommandBuilder AppendClause(string clause)
        {
            if (!string.IsNullOrEmpty(clause))
            {
                this.builder.AppendLine().Append(clause);
            }

            return this;
        }

        public SqlCommand ToPrimaryKeySql(TableSchema schema, object value)
        {
            var primaryKeys = schema.GetPrimaryKeys();
            if (primaryKeys.Length == 1)
            {
                return this.ToCommand(new Dictionary<string, object>
                    {
                        [primaryKeys.Single().ParameterName] = value
                    });
            }

            return this.ToCommand(value);
        }

        public SqlCommand ToCommand(object parameters)
        {
            return new SqlCommand(this.builder.ToString(), parameters);
        }

        public SqlMultipleCommand<T> ToMultipleCommand<T>(IEnumerable<T> parameters)
        {
            return new SqlMultipleCommand<T>(this.builder.ToString(), parameters);
        }
    }
}