// <copyright file="TestDialect.cs" company="Berkeleybross">
// Copyright (c) Berkeleybross. All rights reserved.
// </copyright>
namespace Dapper.MicroCRUD.Tests.Utils
{
    using System;
    using System.Text;
    using Dapper.MicroCRUD.Dialects;
    using Dapper.MicroCRUD.Schema;
    using Pagination;

    /// <summary>
    /// Implementation of <see cref="IDialect"/> for testers
    /// </summary>
    public class TestDialect
        : BaseDialect
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="TestDialect"/> class.
        /// </summary>
        public TestDialect()
            : base("Test")
        {
        }

        /// <inheritdoc />
        public override string MakeInsertReturningIdentityStatement(TableSchema tableSchema)
        {
            var getIdentitySql = "GET IDENTITY";
            return this.MakeInsertStatement(tableSchema) + Environment.NewLine + getIdentitySql;
        }

        public override string MakeGetTopNStatement(TableSchema tableSchema, int take, string conditions, string orderBy)
        {
            var sql = new StringBuilder("SELECT ").AppendSelectPropertiesClause(tableSchema.Columns);
            sql.AppendClause("FROM ").Append(tableSchema.Name);
            sql.AppendClause(conditions);
            if (!string.IsNullOrWhiteSpace(orderBy))
            {
                sql.AppendClause("ORDER BY ").Append(orderBy);
            }

            sql.AppendLine().AppendFormat("TAKE {0}", take);
            return sql.ToString();
        }

        /// <inheritdoc />
        public override string MakeGetPageStatement(TableSchema tableSchema, Page page, string conditions, string orderBy)
        {
            if (string.IsNullOrWhiteSpace(orderBy))
            {
                throw new ArgumentException("orderBy cannot be empty");
            }

            var sql = new StringBuilder("SELECT ").AppendSelectPropertiesClause(tableSchema.Columns);
            sql.AppendClause("FROM ").Append(tableSchema.Name);
            sql.AppendClause(conditions);
            sql.AppendClause("ORDER BY ").Append(orderBy);
            sql.AppendLine().AppendFormat("SKIP {0} TAKE {1}", page.FirstItemIndex, page.PageSize);
            return sql.ToString();
        }

        public override string MakeCreateTempTableStatement(TableSchema tableSchema)
        {
            return "CREATE TEMP TABLE " + tableSchema.Name;
        }

        public override string MakeDropTempTableStatement(TableSchema tableSchema)
        {
            return "DROP TABLE " + tableSchema.Name;
        }

        /// <inheritdoc />
        public override string MakeColumnName(string name)
        {
            return "'" + name + "'";
        }

        /// <inheritdoc />
        public override string MakeTableName(string tableName)
        {
            return "'" + tableName + "'";
        }

        /// <inheritdoc />
        public override string MakeTableName(string schema, string tableName)
        {
            return "'" + schema + "'.'" + tableName + "'";
        }
    }
}