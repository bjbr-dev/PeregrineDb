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
        public static TestDialect Instance { get; } = new TestDialect(new TableSchemaFactory(new TestSqlNameEscaper(),
            new AtttributeTableNameConvention(new TestSqlNameEscaper()), new AttributeColumnNameConvention(new TestSqlNameEscaper()),
            PeregrineConfig.DefaultSqlTypeMapping));

        public TestDialect(TableSchemaFactory tableSchemaFactory)
            : base(tableSchemaFactory)
        {
        }

        /// <inheritdoc />
        public override SqlCommand MakeInsertReturningPrimaryKeyCommand<TPrimaryKey>(object entity)
        {
            Ensure.NotNull(entity, nameof(entity));

            var tableSchema = this.GetTableSchema(entity.GetType());

            if (!tableSchema.CanGeneratePrimaryKey(typeof(TPrimaryKey)))
            {
                throw new InvalidPrimaryKeyException(
                    "Insert<TPrimaryKey>() can only be used for Int32 and Int64 primary keys. Use Insert() for other types of primary keys.");
            }

            Func<ColumnSchema, bool> include = p => p.Usage.IncludeInInsertStatements;
            var columns = tableSchema.Columns;

            var sql = new SqlCommandBuilder("INSERT INTO ").Append(tableSchema.Name).Append(" (").AppendColumnNames(columns, include).Append(")");
            sql.AppendClause("VALUES (").AppendParameterNames(columns, include).Append(")");
            sql.AppendClause("GET IDENTITY");
            sql.AddParameters(entity);
            return sql.ToCommand();
        }

        public override SqlCommand MakeGetFirstNCommand<TEntity>(int take, string orderBy)
        {
            var entityType = typeof(TEntity);
            var tableSchema = this.GetTableSchema(entityType);

            var sql = new SqlCommandBuilder("SELECT ").AppendSelectPropertiesClause(tableSchema.Columns);
            sql.AppendClause("FROM ").Append(tableSchema.Name);
            if (!string.IsNullOrWhiteSpace(orderBy))
            {
                sql.AppendClause("ORDER BY ").Append(orderBy);
            }

            sql.AppendLine().AppendFormat("TAKE {0}", take);
            return new SqlCommand(sql.ToString());
        }

        public override SqlCommand MakeGetFirstNCommand<TEntity>(int take, FormattableString conditions, string orderBy)
        {
            Ensure.NotNull(conditions, nameof(conditions));

            var entityType = typeof(TEntity);
            var tableSchema = this.GetTableSchema(entityType);

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

        public override SqlCommand MakeGetFirstNCommand<TEntity>(int take, object conditions, string orderBy)
        {
            Ensure.NotNull(conditions, nameof(conditions));

            var entityType = typeof(TEntity);
            var tableSchema = this.GetTableSchema(entityType);
            var conditionsSchema = this.GetConditionsSchema(entityType, tableSchema, conditions.GetType());

            var sql = new SqlCommandBuilder("SELECT ").AppendSelectPropertiesClause(tableSchema.Columns);
            sql.AppendClause("FROM ").Append(tableSchema.Name);
            sql.AppendClause(this.MakeWhereClause(conditionsSchema, conditions));
            if (!string.IsNullOrWhiteSpace(orderBy))
            {
                sql.AppendClause("ORDER BY ").Append(orderBy);
            }

            sql.AppendLine().AppendFormat("TAKE {0}", take);
            return new SqlCommand(sql.ToString());
        }

        /// <inheritdoc />
        public override SqlCommand MakeGetPageCommand<TEntity>(Page page, FormattableString conditions, string orderBy)
        {
            if (string.IsNullOrWhiteSpace(orderBy))
            {
                throw new ArgumentException("orderBy cannot be empty");
            }

            var tableSchema = this.GetTableSchema(typeof(TEntity));

            var sql = new SqlCommandBuilder("SELECT ").AppendSelectPropertiesClause(tableSchema.Columns);
            sql.AppendClause("FROM ").Append(tableSchema.Name);
            sql.AppendClause(conditions);
            sql.AppendClause("ORDER BY ").Append(orderBy);
            sql.AppendLine().AppendFormat("SKIP {0} TAKE {1}", page.FirstItemIndex, page.PageSize);
            return sql.ToCommand();
        }

        /// <inheritdoc />
        public override SqlCommand MakeGetPageCommand<TEntity>(Page page, object conditions, string orderBy)
        {
            if (string.IsNullOrWhiteSpace(orderBy))
            {
                throw new ArgumentException("orderBy cannot be empty");
            }

            var entityType = typeof(TEntity);
            var tableSchema = this.GetTableSchema(entityType);
            var conditionsSchema = this.GetConditionsSchema(entityType, tableSchema, conditions.GetType());

            var sql = new SqlCommandBuilder("SELECT ").AppendSelectPropertiesClause(tableSchema.Columns);
            sql.AppendClause("FROM ").Append(tableSchema.Name);
            sql.AppendClause(this.MakeWhereClause(conditionsSchema, conditions));
            sql.AppendClause("ORDER BY ").Append(orderBy);
            sql.AppendLine().AppendFormat("SKIP {0} TAKE {1}", page.FirstItemIndex, page.PageSize);
            return sql.ToCommand();
        }

        public override SqlCommand MakeCreateTempTableCommand<TEntity>()
        {
            var tableSchema = this.GetTableSchema(typeof(TEntity));
            return new SqlCommand("CREATE TEMP TABLE " + tableSchema.Name);
        }

        public override SqlCommand MakeDropTempTableCommand<TEntity>()
        {
            var tableSchema = this.GetTableSchema(typeof(TEntity));
            return new SqlCommand("DROP TABLE " + tableSchema.Name);
        }
    }
}