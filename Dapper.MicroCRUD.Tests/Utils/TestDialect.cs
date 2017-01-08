// <copyright file="TestDialect.cs" company="Berkeleybross">
// Copyright (c) Berkeleybross. All rights reserved.
// </copyright>
namespace Dapper.MicroCRUD.Tests.Utils
{
    using System;
    using System.Text;
    using Dapper.MicroCRUD.Dialects;
    using Dapper.MicroCRUD.Schema;

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

        /// <inheritdoc />
        public override string MakeGetPageStatement(TableSchema tableSchema, int pageNumber, int itemsPerPage, string conditions, string orderBy)
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
            sql.AppendLine().AppendFormat("SKIP {0} TAKE {1}", skip, itemsPerPage);
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