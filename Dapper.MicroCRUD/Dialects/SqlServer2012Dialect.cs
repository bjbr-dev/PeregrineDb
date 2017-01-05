// <copyright file="SqlServer2012Dialect.cs" company="Berkeleybross">
// Copyright (c) Berkeleybross. All rights reserved.
// </copyright>
namespace Dapper.MicroCRUD.Dialects
{
    using System;
    using System.Text;
    using Dapper.MicroCRUD.Schema;

    /// <summary>
    /// Implementation of <see cref="IDialect"/> for SQL Server 2012 and above
    /// </summary>
    public class SqlServer2012Dialect
        : BaseDialect
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="SqlServer2012Dialect"/> class.
        /// </summary>
        public SqlServer2012Dialect()
            : base("SqlServer2012")
        {
        }

        /// <inheritdoc />
        public override string MakeInsertReturningIdentityStatement(TableSchema tableSchema)
        {
            var getIdentitySql = "SELECT CAST(SCOPE_IDENTITY() AS BIGINT) AS [id]";
            return this.MakeInsertStatement(tableSchema) + Environment.NewLine + getIdentitySql;
        }

        /// <inheritdoc />
        public override string MakeGetPageStatement(
            TableSchema tableSchema,
            IDialect dialect,
            int pageNumber,
            int itemsPerPage,
            string conditions,
            string orderBy)
        {
            if (pageNumber < 1)
            {
                throw new ArgumentException("PageNumber is 1-based so must be greater than 0", nameof(pageNumber));
            }

            if (itemsPerPage < 0)
            {
                throw new ArgumentException("ItemsPerPage must be greater than or equal to 0", nameof(itemsPerPage));
            }

            if (string.IsNullOrWhiteSpace(orderBy))
            {
                throw new ArgumentException("orderBy cannot be empty");
            }

            var sql = new StringBuilder("SELECT ").AppendSelectPropertiesClause(tableSchema.Columns);
            sql.AppendClause("FROM ").Append(tableSchema.Name);
            sql.AppendClause(conditions);
            sql.AppendClause("ORDER BY ").Append(orderBy);

            var skip = (pageNumber - 1) * itemsPerPage;
            sql.AppendLine().AppendFormat("OFFSET {0} ROWS FETCH NEXT {1} ROWS ONLY", skip, itemsPerPage);
            return sql.ToString();
        }

        /// <inheritdoc />
        public override string MakeColumnName(string name)
        {
            return "[" + name + "]";
        }

        /// <inheritdoc />
        public override string MakeTableName(string tableName)
        {
            return "[" + tableName + "]";
        }

        /// <inheritdoc />
        public override string MakeTableName(string schema, string tableName)
        {
            return "[" + schema + "].[" + tableName + "]";
        }
    }
}