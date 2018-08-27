// <copyright file="SqlString.cs" company="Berkeleybross">
// Copyright (c) Berkeleybross. All rights reserved.
// </copyright>

namespace PeregrineDb
{
    using System;
    using System.Linq;
    using System.Text;
    using PeregrineDb.Utils;

    public class SqlString
        : FormattableString, IEquatable<FormattableString>
    {
        private readonly object[] arguments;

        public SqlString(string format, params object[] arguments)
        {
            Ensure.NotNull(format, nameof(format));

            this.Format = format;
            this.arguments = arguments ?? new object[0];
        }

        public override string Format { get; }

        public override int ArgumentCount => this.arguments.Length;

        public override object[] GetArguments()
        {
            return this.arguments;
        }

        public override object GetArgument(int index)
        {
            return this.arguments[index];
        }

        public override string ToString(IFormatProvider formatProvider)
        {
            return string.Format(this.Format, this.arguments.Select((v, i) => $"{{{i}={v}}}").ToArray<object>());
        }

        internal static string ParameterizePlaceholders(FormattableString sql)
        {
            if (sql == null)
            {
                return null;
            }

            return ParameterizePlaceholders(sql.Format, sql.ArgumentCount);
        }

        internal static string ParameterizePlaceholders(string format, int numParameters)
        {
            if (format == null)
            {
                throw new ArgumentNullException(nameof(format));
            }

            var index = 0;
            var len = format.Length;

            var result = new StringBuilder();

            while (true)
            {
                char ch;
                while (index < len)
                {
                    ch = format[index++];

                    if (ch == '}')
                    {
                        // Treat as escape character for }}
                        if (index < len && format[index] == '}')
                        {
                            index++;
                        }
                        else
                        {
                            throw new FormatException();
                        }
                    }
                    else if (ch == '{')
                    {
                        // Treat as escape character for {{
                        if (index < len && format[index] == '{')
                        {
                            index++;
                        }
                        else
                        {
                            index--;
                            break;
                        }
                    }

                    result.Append(ch);
                }

                if (index == len)
                {
                    break;
                }

                index++;
                if (index == len || (ch = format[index]) < '0' || ch > '9')
                {
                    throw new FormatException();
                }

                var placeholderPosition = 0;
                do
                {
                    placeholderPosition = (placeholderPosition * 10) + ch - '0';
                    index++;
                    if (index == len)
                    {
                        throw new FormatException();
                    }

                    ch = format[index];
                }
                while (ch >= '0' && ch <= '9' && placeholderPosition < 1000000);

                if (placeholderPosition >= numParameters)
                {
                    throw new FormatException($"Placholder number {placeholderPosition} is more than number of parameters {numParameters}");
                }

                if (ch == ',' || ch == ':')
                {
                    throw new FormatException("Cannot specify custom formatting on parameters to a SQL statement");
                }

                if (ch != '}')
                {
                    throw new FormatException();
                }

                index++;

                result.Append("@p").Append(placeholderPosition);
            }

            return result.ToString();
        }

        public bool Equals(FormattableString other)
        {
            if (ReferenceEquals(null, other))
            {
                return false;
            }

            if (ReferenceEquals(this, other))
            {
                return true;
            }

            return string.Equals(this.Format, other.Format) && this.arguments.SequenceEqual(other.GetArguments());
        }

        public override bool Equals(object obj)
        {
            return this.Equals(obj as FormattableString);
        }

        public override int GetHashCode()
        {
            throw new NotImplementedException();
        }
    }
}
