namespace PeregrineDb
{
    using PeregrineDb.Dialects;

    /// <summary>
    /// Defines the SQL to generate when targeting specific vendor implementations.
    /// </summary>
    public static class Dialect
    {
        /// <summary>
        /// Gets the Dialect for Microsoft SQL Server 2012
        /// </summary>
        public static IDialect SqlServer2012 { get; } = new SqlServer2012Dialect();

        /// <summary>
        /// Gets the dialect for PostgreSQL.
        /// </summary>
        public static IDialect PostgreSql { get; } = new PostgreSqlDialect();
    }
}