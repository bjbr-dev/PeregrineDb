// <copyright file="Dialect.cs" company="Berkeleybross">
//   Copyright (c) Berkeleybross. All rights reserved.
// </copyright>
namespace Dapper.MicroCRUD
{
    /// <summary>
    /// Defines the SQL to generate when targeting specific vendor implementations.
    /// </summary>
    public class Dialect
    {
        private readonly string escapeIdentifierFormat;

        /// <summary>
        /// Initializes a new instance of the <see cref="Dialect"/> class.
        /// </summary>
        /// <param name="name"></param>
        /// <param name="getIdentitySql"></param>
        /// <param name="escapeIdentifierFormat">the format string to use when wrapping a column name to escape reserved characters</param>
        public Dialect(string name, string getIdentitySql, string escapeIdentifierFormat)
        {
            this.GetIdentitySql = getIdentitySql;
            this.escapeIdentifierFormat = escapeIdentifierFormat;
            this.Name = name;
        }

        /// <summary>
        /// Gets the Dialect for Microsoft SQL Server 2012
        /// </summary>
        public static Dialect SqlServer2012 { get; } =
            new Dialect(nameof(SqlServer2012), "SELECT CAST(SCOPE_IDENTITY() AS BIGINT) AS [id]", "[{0}]");

        /// <summary>
        /// Gets the dialect for PostgreSQL.
        /// </summary>
        public static Dialect PostgreSql { get; } =
            new Dialect(nameof(PostgreSql), "SELECT LASTVAL() AS id", "{0}");

        /// <summary>
        /// Gets the name of this Sql Dialect.
        /// </summary>
        public string Name { get; }

        /// <summary>
        /// Gets the SQL statement to use when getting the identity of the last inserted record.
        /// </summary>
        public string GetIdentitySql { get; }

        /// <summary>
        /// Wraps the <paramref name="identifier"/> so that most reserved characters are escaped.
        /// Note: Not all characters can be escaped easily, so we rely on users of this library to use sensible column names that don't need special handling.
        /// </summary>
        public string EscapeMostReservedCharacters(string identifier)
        {
            return string.Format(this.escapeIdentifierFormat, identifier);
        }

        /// <inheritdoc />
        public override string ToString()
        {
            return "Dialect " + this.Name;
        }
    }
}