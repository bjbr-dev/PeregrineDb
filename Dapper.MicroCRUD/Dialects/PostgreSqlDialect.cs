// <copyright file="PostgreSqlDialect.cs" company="Berkeleybross">
// Copyright (c) Berkeleybross. All rights reserved.
// </copyright>
namespace Dapper.MicroCRUD.Dialects
{
    using System;
    using System.Text;
    using Dapper.MicroCRUD.Schema;

    /// <summary>
    /// Implementation of <see cref="IDialect"/> for the PostgreSQL DBMS.
    /// </summary>
    public class PostgreSqlDialect
        : BaseDialect
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="PostgreSqlDialect"/> class.
        /// </summary>
        public PostgreSqlDialect()
            : base("PostgreSql")
        {
        }

        /// <inheritdoc />
        public override string MakeInsertReturningIdentityStatement(TableSchema tableSchema)
        {
            Func<ColumnSchema, bool> include = p => p.Usage.IncludeInInsertStatements;
            var columns = tableSchema.Columns;

            var sql = new StringBuilder("INSERT INTO ")
                .Append(tableSchema.Name)
                .Append(" (").AppendColumnNames(columns, include).Append(")");
            sql.AppendClause("VALUES (").AppendParameterNames(columns, include).Append(")");
            sql.AppendClause("RETURNING ").AppendSelectPropertiesClause(tableSchema.PrimaryKeyColumns);
            return sql.ToString();
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
            sql.AppendLine().AppendFormat("LIMIT {1} OFFSET {0}", skip, itemsPerPage);
            return sql.ToString();
        }

        /// <inheritdoc />
        public override string MakeColumnName(string name)
        {
            return name;
        }

        /// <inheritdoc />
        public override string MakeTableName(string tableName)
        {
            return tableName;
        }

        /// <inheritdoc />
        public override string MakeTableName(string schema, string tableName)
        {
            return schema + "." + tableName;
        }
    }
}