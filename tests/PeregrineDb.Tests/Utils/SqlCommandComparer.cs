namespace PeregrineDb.Tests.Utils
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using FluentAssertions.Equivalency;

    public class SqlCommandComparer
        : IEqualityComparer<SqlCommand>, IEqualityComparer<string>
    {
        private readonly StringComparer comparer;

        public SqlCommandComparer(StringComparer comparer)
        {
            this.comparer = comparer;
        }

        public static SqlCommandComparer Instance { get; } = new SqlCommandComparer(StringComparer.Ordinal);

        public bool Equals(SqlCommand x, SqlCommand y)
        {
            if (!this.Equals(x.CommandText, y.CommandText))
            {
                return false;
            }

            switch (x.Parameters)
            {
                case Dictionary<string, object> xParams when y.Parameters is Dictionary<string, object> yParams:
                {
                    return xParams.OrderBy(kvp => kvp.Key).SequenceEqual(yParams.OrderBy(kvp => kvp.Key));
                }

                default:
                    return Equals(x.Parameters, y.Parameters);
            }
        }

        public int GetHashCode(SqlCommand obj)
        {
            throw new NotSupportedException();
        }

        public bool Equals(string x, string y)
        {
            var xSql = x.Replace("\r\n", "\n").Trim();
            var ySql = y.Replace("\r\n", "\n").Trim();

            return this.comparer.Equals(xSql, ySql);
        }

        public int GetHashCode(string obj)
        {
            throw new NotSupportedException();
        }
    }
}