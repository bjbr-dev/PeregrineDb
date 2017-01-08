// <copyright file="PostgreSqlDialect.cs" company="Berkeleybross">
// Copyright (c) Berkeleybross. All rights reserved.
// </copyright>
namespace Dapper.MicroCRUD.Dialects
{
    using System;
    using System.Collections.Immutable;
    using System.Data;
    using System.Text;
    using Dapper.MicroCRUD.Schema;

    /// <summary>
    /// Implementation of <see cref="IDialect"/> for the PostgreSQL DBMS.
    /// </summary>
    public class PostgreSqlDialect
        : BaseDialect
    {
        private static readonly ImmutableArray<string> ColumnTypes;

        static PostgreSqlDialect()
        {
            var types = new string[28];
            types[(int)DbType.Byte] = null;
            types[(int)DbType.Boolean] = "BOOL";
            types[(int)DbType.Currency] = null;
            types[(int)DbType.Date] = "DATE";
            types[(int)DbType.DateTime] = "TIMESTAMP";
            types[(int)DbType.Decimal] = "NUMERIC";
            types[(int)DbType.Double] = "DOUBLE PRECISION";
            types[(int)DbType.Guid] = "UUID";
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
            types[(int)DbType.Xml] = null;
            types[(int)DbType.DateTime2] = "TIMESTAMP";
            types[(int)DbType.DateTimeOffset] = "TIMESTAMP WITH TIME ZONE";
            ColumnTypes = types.ToImmutableArray();
        }

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
            sql.AppendLine().AppendFormat("LIMIT {1} OFFSET {0}", skip, itemsPerPage);
            return sql.ToString();
        }

        /// <inheritdoc />
        public override string MakeCreateTempTableStatement(TableSchema tableSchema)
        {
            if (tableSchema.Columns.IsEmpty)
            {
                throw new ArgumentException("Temporary tables must have columns");
            }

            var sql = new StringBuilder("CREATE TEMP TABLE ").Append(tableSchema.Name).AppendLine();
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
            sql.Append(")");
            return sql.ToString();
        }

        /// <inheritdoc />
        public override string MakeDropTempTableStatement(TableSchema tableSchema)
        {
            return "DROP TABLE " + tableSchema.Name;
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

        private static string GetColumnType(ColumnSchema column)
        {
            var nullability = column.ColumnType.AllowNull
                ? " NULL"
                : " NOT NULL";

            return GetColumnType(column.ColumnType) + nullability;
        }

        private static string GetColumnType(DbTypeEx dbType)
        {
            switch (dbType.Type)
            {
                case DbType.AnsiStringFixedLength:
                    return "TEXT";
                case DbType.Binary:
                    return "BYTEA";
                case DbType.String:
                    return "TEXT";
                case DbType.StringFixedLength:
                    return "TEXT";
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