namespace PeregrineDb.Tests.Utils
{
    using System;
    using System.Collections.Generic;
    using System.Linq;

    public class SqlStringComparer
        : IEqualityComparer<FormattableString>
    {
        private readonly StringComparer comparer;

        public SqlStringComparer(StringComparer comparer)
        {
            this.comparer = comparer;
        }

        public static SqlStringComparer Instance { get; } = new SqlStringComparer(StringComparer.Ordinal);

        public bool Equals(FormattableString x, FormattableString y)
        {
            var xSql = x?.Format.Replace("\r\n", "\n").Trim();
            var ySql = y?.Format.Replace("\r\n", "\n").Trim();

            if (!this.comparer.Equals(xSql, ySql))
            {
                return false;
            }

            var xArgs = x?.GetArguments() ?? new object[0];
            var yArgs = y?.GetArguments() ?? new object[0];

            return xArgs.SequenceEqual(yArgs);
        }

        public int GetHashCode(FormattableString obj)
        {
            throw new NotSupportedException();
        }
    }
}