// <copyright file="BaseDialect.cs" company="Berkeleybross">
// Copyright (c) Berkeleybross. All rights reserved.
// </copyright>
namespace Dapper.MicroCRUD.Dialects
{
    using System;
    using System.Text;
    using Dapper.MicroCRUD.Schema;

    /// <summary>
    /// Simple implementation of a SQL dialect which performs most SQL generation.
    /// </summary>
    public abstract class BaseDialect
        : IDialect
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="BaseDialect"/> class.
        /// </summary>
        /// <param name="name"></param>
        protected BaseDialect(string name)
        {
            this.Name = name;
        }

        /// <summary>
        /// Gets the name of the dialect
        /// </summary>
        public string Name { get; }

        /// <inheritdoc />
        public string MakeCountStatement(TableSchema tableSchema, string conditions)
        {
            var sql = new StringBuilder("SELECT COUNT(*)");
            sql.AppendClause("FROM ").Append(tableSchema.Name);
            sql.AppendClause(conditions);
            return sql.ToString();
        }

        /// <inheritdoc />
        public string MakeFindStatement(TableSchema tableSchema)
        {
            var sql = new StringBuilder("SELECT ").AppendSelectPropertiesClause(tableSchema.Columns);
            sql.AppendClause("FROM ").Append(tableSchema.Name);
            sql.AppendWherePrimaryKeysClause(tableSchema);
            return sql.ToString();
        }

        /// <inheritdoc />
        public string MakeGetRangeStatement(TableSchema tableSchema, string conditions)
        {
            var sql = new StringBuilder("SELECT ").AppendSelectPropertiesClause(tableSchema.Columns);
            sql.AppendClause("FROM ").Append(tableSchema.Name);
            sql.AppendClause(conditions);
            return sql.ToString();
        }

        /// <inheritdoc />
        public abstract string MakeGetPageStatement(
            TableSchema tableSchema,
            IDialect dialect,
            int pageNumber,
            int itemsPerPage,
            string conditions,
            string orderBy);

        /// <inheritdoc />
        public string MakeInsertStatement(TableSchema tableSchema)
        {
            Func<ColumnSchema, bool> include = p => p.Usage.IncludeInInsertStatements;
            var columns = tableSchema.Columns;

            var sql = new StringBuilder("INSERT INTO ")
                .Append(tableSchema.Name)
                .Append(" (").AppendColumnNames(columns, include).Append(")");
            sql.AppendClause("VALUES (").AppendParameterNames(columns, include).Append(");");
            return sql.ToString();
        }

        /// <inheritdoc />
        public string MakeUpdateStatement(TableSchema tableSchema)
        {
            Func<ColumnSchema, bool> include = p => p.Usage.IncludeInUpdateStatements;

            var sql = new StringBuilder("UPDATE ").Append(tableSchema.Name);
            sql.AppendClause("SET ").AppendColumnNamesEqualParameterNames(tableSchema.Columns, ", ", include);
            sql.AppendWherePrimaryKeysClause(tableSchema);
            return sql.ToString();
        }

        /// <inheritdoc />
        public string MakeDeleteByPrimaryKeyStatement(TableSchema tableSchema)
        {
            var sql = new StringBuilder("DELETE FROM ").Append(tableSchema.Name);
            sql.AppendWherePrimaryKeysClause(tableSchema);
            return sql.ToString();
        }

        /// <summary>
        /// Generates a SQL Delete statement which chooses which row to delete by the <paramref name="conditions"/>.
        /// </summary>
        public string MakeDeleteRangeStatement(TableSchema tableSchema, string conditions)
        {
            var sql = new StringBuilder("DELETE FROM ").Append(tableSchema.Name);
            sql.AppendClause(conditions);
            return sql.ToString();
        }

        /// <inheritdoc />
        public override string ToString()
        {
            return "Dialect " + this.Name;
        }

        /// <inheritdoc />
        public abstract string MakeInsertReturningIdentityStatement(TableSchema tableSchema);

        /// <inheritdoc />
        public abstract string MakeColumnName(string name);

        /// <inheritdoc />
        public abstract string MakeTableName(string tableName);

        /// <inheritdoc />
        public abstract string MakeTableName(string schema, string tableName);
    }
}