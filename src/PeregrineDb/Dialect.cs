// <copyright file="Dialect.cs" company="Berkeleybross">
// Copyright (c) Berkeleybross. All rights reserved.
// </copyright>

namespace PeregrineDb
{
    using PeregrineDb.Dialects;
    using PeregrineDb.Dialects.Postgres;
    using PeregrineDb.Dialects.SqlServer2012;
    using PeregrineDb.Schema;

    /// <summary>
    /// Defines the SQL to generate when targeting specific vendor implementations.
    /// </summary>
    public static class Dialect
    {
        /// <summary>
        /// Gets the Dialect for Microsoft SQL Server 2012.
        /// </summary>
        public static IDialect SqlServer2012 { get; } = new SqlServer2012Dialect(
            new TableSchemaFactory(
                new SqlServer2012NameEscaper(),
                new AtttributeTableNameConvention(new SqlServer2012NameEscaper()),
                new AttributeColumnNameConvention(new SqlServer2012NameEscaper())));

        /// <summary>
        /// Gets the dialect for PostgreSQL.
        /// </summary>
        public static IDialect PostgreSql { get; } = new PostgreSqlDialect(
            new TableSchemaFactory(
                new PostgresNameEscaper(),
                new PostgresAttributeTableNameConvention(new PostgresNameEscaper()),
                new PostgresAttributeColumnNameConvention(new PostgresNameEscaper())));
    }
}