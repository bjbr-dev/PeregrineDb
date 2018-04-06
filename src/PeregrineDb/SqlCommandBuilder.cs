namespace PeregrineDb
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Reflection;
    using System.Text;
    using PeregrineDb.Schema;

    public class SqlCommandBuilder
    {
        private readonly StringBuilder builder = new StringBuilder();
        private Dictionary<string, object> parameters;

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
        /// Appends an arbtitrary clause of SQL to the string. Adds a new line at the start if <paramref name="clause"/> is not empty.
        /// </summary>
        public SqlCommandBuilder AppendClause(string clause)
        {
            if (!string.IsNullOrEmpty(clause))
            {
                this.builder.AppendLine().Append(clause);
            }

            return this;
        }

        /// <summary>
        /// Appends an arbtitrary clause of SQL to the string. Adds a new line at the start if <paramref name="clause"/> is not empty.
        /// Placeholders within the <see cref="FormattableString"/> will be automatically incremented.
        /// </summary>
        public SqlCommandBuilder AppendClause(FormattableString clause)
        {
            if (!string.IsNullOrEmpty(clause?.Format))
            {
                this.builder.AppendLine().Append(SqlString.ParameterizePlaceholders(clause));

                if (clause.ArgumentCount > 0)
                {
                    if (this.parameters != null)
                    {
                        throw new NotImplementedException();
                    }

                    var commandParameters = new Dictionary<string, object>();

                    var i = 0;
                    foreach (var parameter in clause.GetArguments())
                    {
                        commandParameters["p" + i++] = parameter;
                    }

                    this.parameters = commandParameters;
                }
            }

            return this;
        }

        public void AddPrimaryKeyParameter(TableSchema schema, object value)
        {
            var primaryKeys = schema.GetPrimaryKeys();
            if (primaryKeys.Length == 1)
            {
                this.parameters = new Dictionary<string, object>
                    {
                        [primaryKeys.Single().ParameterName] = value
                    };
            }
            else
            {
                this.AddParameters(value);
            }
        }

        public void AddParameters(object value)
        {
            if (value == null)
            {
                return;
            }

            if (this.parameters != null)
            {
                throw new NotImplementedException();
            }

            var commandParameters = new Dictionary<string, object>();

            foreach (var property in value.GetType().GetTypeInfo().DeclaredProperties.Where(p => p.GetIndexParameters().Length == 0))
            {
                commandParameters[property.Name] = property.GetValue(value);
            }

            this.parameters = commandParameters;
        }

        public SqlCommand ToCommand()
        {
            return new SqlCommand(this.builder.ToString(), this.parameters);
        }

        public SqlCommand ToCommand(object commandParameters)
        {
            if (this.parameters != null)
            {
                throw new NotImplementedException();
            }

            return new SqlCommand(this.builder.ToString(), commandParameters);
        }

        public static SqlCommand MakeCommand(FormattableString sql)
        {
            var commandParameters = new Dictionary<string, object>();

            var i = 0;
            var arguments = sql.GetArguments();
            foreach (var parameter in arguments)
            {
                commandParameters["p" + i++] = parameter;
            }

            return new SqlCommand(SqlString.ParameterizePlaceholders(sql.Format, arguments.Length), commandParameters);
        }
    }
}