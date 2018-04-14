namespace PeregrineDb.Tests.Databases
{
    using System.Collections.Generic;

    public abstract partial class DefaultDatabaseConnectionStatementsTests
    {
        public static IEnumerable<object[]> TestDialects => new[]
            {
                new[] { Dialect.SqlServer2012 },
                new[] { Dialect.PostgreSql }
            };
    }
}