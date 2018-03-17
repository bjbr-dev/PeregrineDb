// <copyright file="SqlServer2012Dialect.cs" company="Berkeleybross">
// Copyright (c) Berkeleybross. All rights reserved.
// </copyright>
namespace Dapper.MicroCRUD.Dialects
{
    using System;
    using System.Collections.Immutable;
    using System.Data;
    using System.Text;
    using Dapper.MicroCRUD.Schema;
    using Dapper.MicroCRUD.Schema.Relations;
    using Pagination;

    /// <summary>
    /// Implementation of <see cref="IDialect"/> for SQL Server 2012 and above
    /// </summary>
    public class SqlServer2012Dialect
        : BaseDialect, ISchemaQueryDialect
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
            : base("SqlServer2012")
        {
        }

        /// <inheritdoc />
        public override string MakeInsertReturningIdentityStatement(TableSchema tableSchema)
        {
            var getIdentitySql = "SELECT CAST(SCOPE_IDENTITY() AS BIGINT) AS [id]";
            return this.MakeInsertStatement(tableSchema) + Environment.NewLine + getIdentitySql;
        }

        public override string MakeGetTopNStatement(TableSchema tableSchema, int take, string conditions, string orderBy)
        {
            var sql = new StringBuilder("SELECT TOP ").Append(take).Append(" ").AppendSelectPropertiesClause(tableSchema.Columns);
            sql.AppendClause("FROM ").Append(tableSchema.Name);
            sql.AppendClause(conditions);
            if (!string.IsNullOrWhiteSpace(orderBy))
            {
                sql.AppendClause("ORDER BY ").Append(orderBy);
            }

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
            sql.AppendLine().AppendFormat("OFFSET {0} ROWS FETCH NEXT {1} ROWS ONLY", page.FirstItemIndex, page.PageSize);
            return sql.ToString();
        }

        /// <inheritdoc />
        public override string MakeCreateTempTableStatement(TableSchema tableSchema)
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
            return sql.ToString();
        }

        /// <inheritdoc />
        public override string MakeDropTempTableStatement(TableSchema tableSchema)
        {
            EnsureValidSchemaForTempTables(tableSchema);

            return "DROP TABLE " + tableSchema.Name;
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

        public string MakeGetAllTablesStatement()
        {
            return @"
SELECT TABLE_SCHEMA + '.' + TABLE_NAME AS Name
FROM  INFORMATION_SCHEMA.TABLES
WHERE TABLE_TYPE='BASE TABLE'";
        }

        public string MakeGetAllRelationsStatement()
        {
            return @"
SELECT ReferencedTable = OBJECT_SCHEMA_NAME(pt.referenced_object_id) + '.' + OBJECT_NAME(pt.referenced_object_id),
       ReferencingTable = OBJECT_SCHEMA_NAME(pt.parent_object_id) + '.' + OBJECT_NAME(pt.parent_object_id),
       ReferencingColumn = pc.name,
       RelationIsOptional = pc.is_nullable
FROM sys.foreign_key_columns AS pt
INNER JOIN sys.columns AS pc ON pt.parent_object_id = pc.[object_id] AND pt.parent_column_id = pc.column_id
INNER JOIN sys.columns AS rc ON pt.referenced_column_id = rc.column_id AND pt.referenced_object_id = rc.[object_id]";
        }

        public string MakeSetColumnNullStatement(string tableName, string columnName)
        {
            var sql = new StringBuilder("UPDATE ").Append(tableName);
            sql.AppendClause("SET ").Append(columnName).Append(" = NULL");
            return sql.ToString();
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