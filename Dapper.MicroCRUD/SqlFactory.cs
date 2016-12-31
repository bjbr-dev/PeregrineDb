// <copyright file="SqlFactory.cs" company="Berkeleybross">
// Copyright (c) Berkeleybross. All rights reserved.
// </copyright>
namespace Dapper.MicroCRUD
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using Dapper.MicroCRUD.Entities;

    /// <summary>
    /// Helper methods to generate SQL statements
    /// </summary>
    public static class SqlFactory
    {
        /// <summary>
        /// Generates a SQL Select statement which counts how many rows match the <paramref name="conditions"/>.
        /// </summary>
        public static string MakeCountStatement(TableSchema tableSchema, string conditions)
        {
            var sql = new StringBuilder("SELECT COUNT(*)");
            sql.AppendClause("FROM ").Append(tableSchema.Name);
            sql.AppendClause(conditions);
            return sql.ToString();
        }

        /// <summary>
        /// Generates a SQL statement to select a single row from a table.
        /// Requires a single parameter called @Id
        /// </summary>
        public static string MakeFindStatement(TableSchema tableSchema)
        {
            var primaryKey = tableSchema.GetSinglePrimaryKey();

            var sql = new StringBuilder("SELECT ").AppendSelectPropertiesClause(tableSchema.Columns);
            sql.AppendClause("FROM ").Append(tableSchema.Name);
            sql.AppendClause("WHERE ").Append(primaryKey.ColumnName).Append(" = @Id");
            return sql.ToString();
        }

        /// <summary>
        /// Generates a SQL statement to select multiple rows.
        /// </summary>
        public static string MakeGetRangeStatement(TableSchema tableSchema, string conditions)
        {
            var sql = new StringBuilder("SELECT ").AppendSelectPropertiesClause(tableSchema.Columns);
            sql.AppendClause("FROM ").Append(tableSchema.Name);
            sql.AppendClause(conditions);
            return sql.ToString();
        }

        /// <summary>
        /// Generates a SQL statement to insert a row and return the generated identity.
        /// </summary>
        public static string MakeInsertStatement(TableSchema tableSchema)
        {
            var sql = new StringBuilder("INSERT INTO ")
                .Append(tableSchema.Name)
                .Append(" (").AppendInsertParametersClause(tableSchema.Columns).Append(")");
            sql.AppendClause("VALUES (").AppendInsertValuesClause(tableSchema.Columns).Append(");");
            return sql.ToString();
        }

        /// <summary>
        /// Generates a SQL statement to insert a row and return the generated identity.
        /// </summary>
        public static string MakeInsertReturningIdentityStatement(TableSchema tableSchema, Dialect dialect)
        {
            return MakeInsertStatement(tableSchema) + Environment.NewLine + dialect.GetIdentitySql;
        }

        /// <summary>
        /// Generates a SQL Update statement which chooses which row to update by its PrimaryKey.
        /// </summary>
        public static string MakeUpdateStatement(TableSchema tableSchema)
        {
            var primaryKey = tableSchema.GetSinglePrimaryKey();

            var sql = new StringBuilder("UPDATE ").Append(tableSchema.Name);
            sql.AppendClause("SET ").AppendUpdateSetClause(tableSchema.Columns);
            sql.AppendClause("WHERE ").Append(primaryKey.ColumnName).Append(" = @").Append(primaryKey.ParameterName);
            return sql.ToString();
        }

        /// <summary>
        /// Generates a SQL Delete statement which chooses which row to delete its PrimaryKey.
        /// </summary>
        public static string MakeDeleteByPrimaryKeyStatement(TableSchema tableSchema)
        {
            var primaryKey = tableSchema.GetSinglePrimaryKey();

            var sql = new StringBuilder("DELETE FROM ").Append(tableSchema.Name);
            sql.AppendClause("WHERE ").Append(primaryKey.ColumnName).Append(" = @").Append(primaryKey.ParameterName);
            return sql.ToString();
        }

        /// <summary>
        /// Generates a SQL Delete statement which chooses which row to delete by the <paramref name="conditions"/>.
        /// </summary>
        public static string MakeDeleteRangeStatement(TableSchema tableSchema, string conditions)
        {
            var sql = new StringBuilder("DELETE FROM ").Append(tableSchema.Name);
            sql.AppendClause(conditions);
            return sql.ToString();
        }

        private static StringBuilder AppendSelectPropertiesClause(
            this StringBuilder sql,
            IEnumerable<ColumnSchema> properties)
        {
            var isFirst = true;
            foreach (var property in properties)
            {
                if (!isFirst)
                {
                    sql.Append(", ");
                }

                sql.Append(property.ColumnName);

                if (property.ColumnName != property.SelectName)
                {
                    sql.Append(" AS " + property.SelectName);
                }

                isFirst = false;
            }

            return sql;
        }

        private static StringBuilder AppendUpdateSetClause(this StringBuilder sql, IEnumerable<ColumnSchema> properties)
        {
            var isFirst = true;
            foreach (var property in properties.Where(p => !p.IsPrimaryKey))
            {
                if (!isFirst)
                {
                    sql.Append(", ");
                }

                sql.Append(property.ColumnName).Append(" = @").Append(property.ParameterName);
                isFirst = false;
            }

            return sql;
        }

        private static StringBuilder AppendInsertValuesClause(
            this StringBuilder sql,
            IEnumerable<ColumnSchema> properties)
        {
            var isFirst = true;
            foreach (var property in properties.Where(p => !p.IsGeneratedByDatabase))
            {
                if (!isFirst)
                {
                    sql.Append(", ");
                }

                sql.Append("@").Append(property.ParameterName);
                isFirst = false;
            }

            return sql;
        }

        private static StringBuilder AppendInsertParametersClause(
            this StringBuilder sql,
            IEnumerable<ColumnSchema> properties)
        {
            var isFirst = true;
            foreach (var property in properties.Where(p => !p.IsGeneratedByDatabase))
            {
                if (!isFirst)
                {
                    sql.Append(", ");
                }

                sql.Append(property.ColumnName);
                isFirst = false;
            }

            return sql;
        }

        private static StringBuilder AppendClause(this StringBuilder sql, string clause)
        {
            return string.IsNullOrEmpty(clause)
                ? sql
                : sql.AppendLine().Append(clause);
        }
    }
}