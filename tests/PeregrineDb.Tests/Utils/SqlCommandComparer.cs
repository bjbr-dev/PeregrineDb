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
            var xSql = x.Text.Replace("\r\n", "\n").Trim();
            var ySql = y.Text.Replace("\r\n", "\n").Trim();

            if (!this.comparer.Equals(xSql, ySql))
            {
                return false;
            }

            var xArgs = x.Parameters ?? new Dictionary<string, object>();
            var yArgs = y.Parameters ?? new Dictionary<string, object>();
            
            return xArgs.OrderBy(z => z.Key).SequenceEqual(yArgs.OrderBy(z => z.Key));
        }

        public int GetHashCode(SqlCommand obj)
        {
            throw new NotSupportedException();
        }
    }
}