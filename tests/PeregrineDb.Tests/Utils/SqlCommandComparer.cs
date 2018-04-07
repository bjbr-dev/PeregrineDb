namespace PeregrineDb.Tests.Utils
{
    using System;
    using System.Collections.Generic;
    using System.Linq;

    public class SqlCommandComparer
        : IEqualityComparer<SqlCommand>
    {
        private readonly StringComparer comparer;

        public SqlCommandComparer(StringComparer comparer)
        {
            this.comparer = comparer;
        }

        public static SqlCommandComparer Instance { get; } = new SqlCommandComparer(StringComparer.Ordinal);

        public bool Equals(SqlCommand x, SqlCommand y)
        {
            var xSql = x.CommandText.Replace("\r\n", "\n").Trim();
            var ySql = y.CommandText.Replace("\r\n", "\n").Trim();

            if (!this.comparer.Equals(xSql, ySql))
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
    }
}