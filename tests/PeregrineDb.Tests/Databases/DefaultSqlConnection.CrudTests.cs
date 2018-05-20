namespace PeregrineDb.Tests.Databases
{
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;

    [SuppressMessage("ReSharper", "StringLiteralAsInterpolationArgument")]
    public abstract partial class DefaultDatabaseConnectionCrudTests
    {
        [SuppressMessage("ReSharper", "MemberCanBePrivate.Global")]
        public static IEnumerable<object[]> TestDialects => new[]
            {
                new[] { Dialect.SqlServer2012 },
                new[] { Dialect.PostgreSql }
            };

        [SuppressMessage("ReSharper", "MemberCanBePrivate.Global")]
        public static IEnumerable<object[]> TestDialectsWithData(string data) => new[]
            {
                new object[] { Dialect.SqlServer2012, data },
                new object[] { Dialect.PostgreSql, data }
            };

    }
}