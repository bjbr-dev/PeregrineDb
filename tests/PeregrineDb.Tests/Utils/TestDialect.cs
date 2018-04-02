namespace PeregrineDb.Tests.Utils
{
    using System;
    using Pagination;
    using PeregrineDb.Dialects;
    using PeregrineDb.Schema;

    /// <summary>
    /// Implementation of <see cref="IDialect"/> for testers
    /// </summary>
    public class TestDialect
        : StandardDialect
    {
        /// <inheritdoc />
        public override SqlCommand MakeInsertReturningIdentityStatement(TableSchema tableSchema, object entity)
        {
            var getIdentitySql = "GET IDENTITY";
            return new SqlCommand(this.MakeInsertCommand(tableSchema, entity) + Environment.NewLine + getIdentitySql);
        }

        public override SqlCommand MakeGetTopNStatement(TableSchema tableSchema, int take, FormattableString conditions, string orderBy)
        {
            var sql = new SqlCommandBuilder("SELECT ").AppendSelectPropertiesClause(tableSchema.Columns);
            sql.AppendClause("FROM ").Append(tableSchema.Name);
            sql.AppendClause(conditions);
            if (!string.IsNullOrWhiteSpace(orderBy))
            {
                sql.AppendClause("ORDER BY ").Append(orderBy);
            }

            sql.AppendLine().AppendFormat("TAKE {0}", take);
            return new SqlCommand(sql.ToString());
        }

        /// <inheritdoc />
        public override SqlCommand MakeGetPageStatement(TableSchema tableSchema, Page page, FormattableString conditions, string orderBy)
        {
            if (string.IsNullOrWhiteSpace(orderBy))
            {
                throw new ArgumentException("orderBy cannot be empty");
            }

            var sql = new SqlCommandBuilder("SELECT ").AppendSelectPropertiesClause(tableSchema.Columns);
            sql.AppendClause("FROM ").Append(tableSchema.Name);
            sql.AppendClause(conditions);
            sql.AppendClause("ORDER BY ").Append(orderBy);
            sql.AppendLine().AppendFormat("SKIP {0} TAKE {1}", page.FirstItemIndex, page.PageSize);
            return sql.ToCommand();
        }

        public override SqlCommand MakeCreateTempTableStatement(TableSchema tableSchema)
        {
            return new SqlCommand("CREATE TEMP TABLE " + tableSchema.Name);
        }

        public override SqlCommand MakeDropTempTableStatement(TableSchema tableSchema)
        {
            return new SqlCommand("DROP TABLE " + tableSchema.Name);
        }
    }
}