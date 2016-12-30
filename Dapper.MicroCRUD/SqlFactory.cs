// <copyright file="SqlFactory.cs" company="Berkeleybross">
// Copyright (c) Berkeleybross. All rights reserved.
// </copyright>
namespace Dapper.MicroCRUD
{
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using Dapper.MicroCRUD.Entities;

    /// <summary>
    /// Helper methods to generate SQL statements
    /// </summary>
    internal static class SqlFactory
    {
        /// <summary>
        /// Generates a SQL Select statement which counts how many rows match the <paramref name="conditions"/>.
        /// </summary>
        public static string MakeCountStatement(TableSchema tableSchema, string conditions)
        {
            var sql = new StringBuilder();
            sql.AppendLine("SELECT COUNT(*)");
            sql.Append("FROM ").AppendLine(tableSchema.Name);
            sql.AppendLine(conditions);

            return sql.ToString();
        }

        /// <summary>
        /// Generates a SQL statement to select a single row from a table.
        /// Requires a single parameter called @Id
        /// </summary>
        public static string MakeFindStatement(TableSchema tableSchema, ColumnSchema primaryKey, Dialect dialect)
        {
            var sql = new StringBuilder();
            sql.Append("SELECT ").AppendSelectPropertiesClause(tableSchema.Columns, dialect).AppendLine();
            sql.Append("FROM ").AppendLine(tableSchema.Name);
            sql.Append("WHERE ").Append(primaryKey.ColumnName).AppendLine(" = @Id");
            return sql.ToString();
        }

        /// <summary>
        /// Generates a SQL statement to select multiple rows.
        /// </summary>
        public static string MakeGetRangeStatement(TableSchema tableSchema, string conditions, Dialect dialect)
        {
            var sql = new StringBuilder();
            sql.Append("SELECT ")
               .AppendSelectPropertiesClause(tableSchema.Columns, dialect)
               .AppendLine();
            sql.Append("FROM ").AppendLine(tableSchema.Name);
            sql.Append(conditions);

            return sql.ToString();
        }

        /// <summary>
        /// Generates a SQL statement to insert a row and return the generated identity.
        /// </summary>
        public static string MakeInsertStatement(TableSchema tableSchema)
        {
            var sql = new StringBuilder();

            sql.Append("INSERT INTO ").Append(tableSchema.Name);
            sql.Append(" (").AppendInsertParametersClause(tableSchema.Columns).AppendLine(")");
            sql.Append("VALUES (").AppendInsertValuesClause(tableSchema.Columns).AppendLine(");");

            return sql.ToString();
        }

        /// <summary>
        /// Generates a SQL statement to insert a row and return the generated identity.
        /// </summary>
        public static string MakeInsertReturningIdentityStatement(TableSchema tableSchema, Dialect dialect)
        {
            var sql = new StringBuilder();

            sql.Append("INSERT INTO ").Append(tableSchema.Name);
            sql.Append(" (").AppendInsertParametersClause(tableSchema.Columns).AppendLine(")");
            sql.Append("VALUES (").AppendInsertValuesClause(tableSchema.Columns).AppendLine(");");

            sql.Append(dialect.GetIdentitySql);

            return sql.ToString();
        }

        /// <summary>
        /// Generates a SQL Update statement which chooses which row to update by the <paramref name="primaryKey"/>.
        /// </summary>
        public static string MakeUpdateStatement(TableSchema tableSchema, ColumnSchema primaryKey)
        {
            var sql = new StringBuilder();
            sql.Append("UPDATE ").AppendLine(tableSchema.Name);
            sql.Append("SET ").AppendUpdateSetClause(tableSchema.Columns).AppendLine();
            sql.Append("WHERE ").Append(primaryKey.ColumnName).Append(" = @").Append(primaryKey.PropertyName);
            return sql.ToString();
        }

        /// <summary>
        /// Generates a SQL Delete statement which chooses which row to delete by the <paramref name="primaryKey"/>.
        /// </summary>
        public static string MakeDeleteByPrimaryKeyStatement(TableSchema tableSchema, ColumnSchema primaryKey)
        {
            var sql = new StringBuilder();
            sql.Append("DELETE FROM ").AppendLine(tableSchema.Name);
            sql.Append("WHERE ").Append(primaryKey.ColumnName).Append(" = @").Append(primaryKey.PropertyName);
            return sql.ToString();
        }

        /// <summary>
        /// Generates a SQL Delete statement which chooses which row to delete by the <paramref name="conditions"/>.
        /// </summary>
        public static string MakeDeleteRangeStatement(TableSchema tableSchema, string conditions)
        {
            var sql = new StringBuilder();
            sql.AppendFormat("DELETE FROM ").AppendLine(tableSchema.Name);
            sql.Append(conditions);

            return sql.ToString();
        }

        private static StringBuilder AppendSelectPropertiesClause(
            this StringBuilder sql,
            IEnumerable<ColumnSchema> properties,
            Dialect dialect)
        {
            var isFirst = true;
            foreach (var property in properties)
            {
                if (!isFirst)
                {
                    sql.Append(",");
                }

                sql.Append(property.ColumnName);

                if (property.ColumnName != property.PropertyName)
                {
                    sql.Append(" AS " + dialect.EscapeMostReservedCharacters(property.PropertyName));
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

                sql.Append(property.ColumnName).Append(" = @").Append(property.PropertyName);
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

                sql.Append("@").Append(property.PropertyName);
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
    }
}