namespace PeregrineDb.Tests.Utils
{
    using System;
    using Pagination;
    using PeregrineDb.Dialects;
    using PeregrineDb.Schema;
    using PeregrineDb.Utils;

    /// <summary>
    /// Implementation of <see cref="IDialect"/> for testers
    /// </summary>
    public class TestDialect
        : StandardDialect
    {
        public static TestDialect Instance { get; } = new TestDialect();

        /// <inheritdoc />
        public override SqlCommand MakeInsertReturningPrimaryKeyCommand(object entity, TableSchema tableSchema)
        {
            Func<ColumnSchema, bool> include = p => p.Usage.IncludeInInsertStatements;
            var columns = tableSchema.Columns;

            var sql = new SqlCommandBuilder("INSERT INTO ").Append(tableSchema.Name).Append(" (").AppendColumnNames(columns, include).Append(")");
            sql.AppendClause("VALUES (").AppendParameterNames(columns, include).Append(")");
            sql.AppendClause("GET IDENTITY");
            return sql.ToCommand(entity);
        }

        public override SqlCommand MakeGetFirstNCommand(int take, string conditions, object parameters, string orderBy, TableSchema tableSchema)
        {
            Ensure.NotNull(conditions, nameof(conditions));

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
        public override SqlCommand MakeGetPageCommand(Page page, string conditions, object parameters, string orderBy, TableSchema tableSchema)
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
            return sql.ToCommand(parameters);
        }

        public override SqlCommand MakeCreateTempTableCommand(TableSchema tableSchema)
        {
            return new SqlCommand("CREATE TEMP TABLE " + tableSchema.Name);
        }

        public override SqlCommand MakeDropTempTableCommand(TableSchema tableSchema)
        {
            return new SqlCommand("DROP TABLE " + tableSchema.Name);
        }
    }
}