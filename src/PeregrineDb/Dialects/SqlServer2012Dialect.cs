namespace PeregrineDb.Dialects
{
    using System;
    using System.Collections.Immutable;
    using System.Data;
    using System.Text;
    using Pagination;
    using PeregrineDb.Schema;
    using PeregrineDb.Schema.Relations;

    /// <summary>
    /// Implementation of <see cref="IDialect"/> for SQL Server 2012 and above
    /// </summary>
    public class SqlServer2012Dialect
        : StandardDialect, ISchemaQueryDialect
    {
        private static readonly ImmutableArray<string> ColumnTypes;

        static SqlServer2012Dialect()
        {
            var types = new string[28];
            types[(int)DbType.Byte] = null;
            types[(int)DbType.Boolean] = "BIT";
            types[(int)DbType.Currency] = null;
            types[(int)DbType.Date] = "DATE";
            types[(int)DbType.DateTime] = "DATETIME";
            types[(int)DbType.Decimal] = "NUMERIC";
            types[(int)DbType.Double] = "FLOAT";
            types[(int)DbType.Guid] = "UNIQUEIDENTIFIER";
            types[(int)DbType.Int16] = "SMALLINT";
            types[(int)DbType.Int32] = "INT";
            types[(int)DbType.Int64] = "BIGINT";
            types[(int)DbType.Object] = null;
            types[(int)DbType.SByte] = null;
            types[(int)DbType.Single] = "REAL";
            types[(int)DbType.Time] = "TIME";
            types[(int)DbType.UInt16] = null;
            types[(int)DbType.UInt32] = null;
            types[(int)DbType.UInt64] = null;
            types[(int)DbType.VarNumeric] = null;
            types[(int)DbType.Xml] = "XML";
            types[(int)DbType.DateTime2] = "DATETIME2(7)";
            types[(int)DbType.DateTimeOffset] = "DATETIMEOFFSET";
            ColumnTypes = types.ToImmutableArray();
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="SqlServer2012Dialect"/> class.
        /// </summary>
        public SqlServer2012Dialect()
            : base()
        {
        }

        /// <inheritdoc />
        public override FormattableString MakeInsertReturningIdentityStatement(TableSchema tableSchema, object entity)
        {
            var sql = this.MakeInsertStatement(tableSchema, entity);
            return new SqlString(sql.Format + Environment.NewLine + "SELECT CAST(SCOPE_IDENTITY() AS BIGINT) AS [id]", sql.GetArguments());
        }

        public override FormattableString MakeGetTopNStatement(TableSchema tableSchema, int take, FormattableString conditions, string orderBy)
        {
            var sql = new StringBuilder("SELECT TOP ").Append(take).Append(" ").AppendSelectPropertiesClause(tableSchema.Columns);
            sql.AppendClause("FROM ").Append(tableSchema.Name);
            sql.AppendClause(conditions);
            if (!string.IsNullOrWhiteSpace(orderBy))
            {
                sql.AppendClause("ORDER BY ").Append(orderBy);
            }

            return new SqlString(sql.ToString(), conditions?.GetArguments());
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
            sql.AppendLine().AppendFormat("OFFSET {0} ROWS FETCH NEXT {1} ROWS ONLY", page.FirstItemIndex, page.PageSize);
            return new SqlString(sql.ToString(), conditions?.GetArguments());
        }

        /// <inheritdoc />
        public override FormattableString MakeCreateTempTableStatement(TableSchema tableSchema)
        {
            EnsureValidSchemaForTempTables(tableSchema);

            var sql = new StringBuilder("CREATE TABLE ").Append(tableSchema.Name).AppendLine();
            sql.AppendLine("(");

            var isFirst = true;
            foreach (var column in tableSchema.Columns)
            {
                if (!isFirst)
                {
                    sql.AppendLine(",");
                }

                sql.Append(new string(' ', 4));
                sql.Append(column.ColumnName);
                sql.Append(" ").Append(GetColumnType(column));

                isFirst = false;
            }

            sql.AppendLine();
            sql.Append(");");
            return new SqlString(sql.ToString());
        }

        /// <inheritdoc />
        public override FormattableString MakeDropTempTableStatement(TableSchema tableSchema)
        {
            EnsureValidSchemaForTempTables(tableSchema);

            return new SqlString("DROP TABLE " + tableSchema.Name);
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

        public FormattableString MakeGetAllTablesStatement()
        {
            return new SqlString(@"
SELECT TABLE_SCHEMA + '.' + TABLE_NAME AS Name
FROM  INFORMATION_SCHEMA.TABLES
WHERE TABLE_TYPE='BASE TABLE'");
        }

        public FormattableString MakeGetAllRelationsStatement()
        {
            return new SqlString(@"
SELECT OBJECT_SCHEMA_NAME(foreign_key.referenced_object_id) + '.' + OBJECT_NAME(foreign_key.referenced_object_id) AS TargetTable,
       OBJECT_SCHEMA_NAME(foreign_key.parent_object_id) + '.' + OBJECT_NAME(foreign_key.parent_object_id) AS SourceTable,
       primary_column.name AS SourceColumn,
       primary_column.is_nullable AS SourceColumnIsOptional
FROM sys.foreign_key_columns AS foreign_key
INNER JOIN sys.columns AS primary_column ON foreign_key.parent_object_id = primary_column.[object_id] AND foreign_key.parent_column_id = primary_column.column_id
INNER JOIN sys.columns AS foreign_column ON foreign_key.referenced_column_id = foreign_column.column_id AND foreign_key.referenced_object_id = foreign_column.[object_id]");
        }

        public FormattableString MakeSetColumnNullStatement(string tableName, string columnName)
        {
            var sql = new StringBuilder("UPDATE ").Append(tableName);
            sql.AppendClause("SET ").Append(columnName).Append(" = NULL");
            return new SqlString(sql.ToString());
        }

        private static void EnsureValidSchemaForTempTables(TableSchema tableSchema)
        {
            if (!tableSchema.Name.StartsWith("#") && !tableSchema.Name.StartsWith("[#"))
            {
                throw new ArgumentException("Temporary table names must begin with a #");
            }

            if (tableSchema.Columns.IsEmpty)
            {
                throw new ArgumentException("Temporary tables must have columns");
            }
        }

        private static string GetColumnType(ColumnSchema column)
        {
            var nullability = column.ColumnType.AllowNull
                ? " NULL"
                : " NOT NULL";

            return GetColumnType(column.ColumnType) + nullability;
        }

        private static string GetColumnType(DbTypeEx dbType)
        {
            var length = dbType.MaxLength?.ToString() ?? "MAX";

            switch (dbType.Type)
            {
                case DbType.AnsiStringFixedLength:
                    return "CHAR(" + length + ")";
                case DbType.Binary:
                    return "VARBINARY(" + length + ")";
                case DbType.String:
                    return "NVARCHAR(" + length + ")";
                case DbType.StringFixedLength:
                    return "NCHAR(" + length + ")";
                default:
                    var index = (int)dbType.Type;
                    if (index >= 0 && index < ColumnTypes.Length)
                    {
                        var result = ColumnTypes[index];
                        if (result != null)
                        {
                            return result;
                        }
                    }

                    throw new NotSupportedException("Unknown DbType: " + dbType.Type);
            }
        }
    }
}