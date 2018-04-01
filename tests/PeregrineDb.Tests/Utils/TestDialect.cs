namespace PeregrineDb.Tests.Utils
{
    using System;
    using System.Text;
    using Pagination;
    using PeregrineDb.Dialects;
    using PeregrineDb.Schema;
    using PeregrineDb.SqlCommands;

    /// <summary>
    /// Implementation of <see cref="IDialect"/> for testers
    /// </summary>
    public class TestDialect
        : StandardDialect
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="TestDialect"/> class.
        /// </summary>
        public TestDialect()
            : base("Test")
        {
        }

        /// <inheritdoc />
        public override FormattableString MakeInsertReturningIdentityStatement(TableSchema tableSchema, object entity)
        {
            var getIdentitySql = "GET IDENTITY";
            return new SqlString(this.MakeInsertStatement(tableSchema, entity) + Environment.NewLine + getIdentitySql);
        }

        public override FormattableString MakeGetTopNStatement(TableSchema tableSchema, int take, FormattableString conditions, string orderBy)
        {
            var sql = new StringBuilder("SELECT ").AppendSelectPropertiesClause(tableSchema.Columns);
            sql.AppendClause("FROM ").Append(tableSchema.Name);
            sql.AppendClause(conditions);
            if (!string.IsNullOrWhiteSpace(orderBy))
            {
                sql.AppendClause("ORDER BY ").Append(orderBy);
            }

            sql.AppendLine().AppendFormat("TAKE {0}", take);
            return new SqlString(sql.ToString());
        }

        /// <inheritdoc />
        public override FormattableString MakeGetPageStatement(TableSchema tableSchema, Page page, FormattableString conditions, string orderBy)
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
            return new SqlString(sql.ToString(), conditions.GetArguments());
        }

        public override FormattableString MakeCreateTempTableStatement(TableSchema tableSchema)
        {
            return new SqlString("CREATE TEMP TABLE " + tableSchema.Name);
        }

        public override FormattableString MakeDropTempTableStatement(TableSchema tableSchema)
        {
            return new SqlString("DROP TABLE " + tableSchema.Name);
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