// <copyright file="SqlFactory.cs" company="Berkeleybross">
// Copyright (c) Berkeleybross. All rights reserved.
// </copyright>
namespace Dapper.MicroCRUD
{
    using System;
    using System.Collections.Immutable;
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
        /// </summary>
        public static string MakeFindStatement(TableSchema tableSchema)
        {
            var sql = new StringBuilder("SELECT ").AppendSelectPropertiesClause(tableSchema.Columns);
            sql.AppendClause("FROM ").Append(tableSchema.Name);
            sql.AppendWherePrimaryKeysClause(tableSchema);
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
        /// Generates a SQL statement to select a page of rows, in a specific order
        /// </summary>
        public static string MakeGetPageStatement(
            TableSchema tableSchema,
            Dialect dialect,
            int pageNumber,
            int itemsPerPage,
            string conditions,
            string orderBy)
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
            sql.AppendLine().AppendFormat(dialect.PagerFormat, (pageNumber - 1) * itemsPerPage, itemsPerPage);
            return sql.ToString();
        }

        /// <summary>
        /// Generates a SQL statement to insert a row and return the generated identity.
        /// </summary>
        public static string MakeInsertStatement(TableSchema tableSchema)
        {
            Func<ColumnSchema, bool> include = p => p.Usage.IncludeInInsertStatements;
            var columns = tableSchema.Columns;

            var sql = new StringBuilder("INSERT INTO ")
                .Append(tableSchema.Name)
                .Append(" (").AppendColumnNames(columns, include).Append(")");
            sql.AppendClause("VALUES (").AppendParameterNames(columns, include).Append(");");
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
            Func<ColumnSchema, bool> include = p => p.Usage.IncludeInUpdateStatements;

            var sql = new StringBuilder("UPDATE ").Append(tableSchema.Name);
            sql.AppendClause("SET ").AppendColumnNamesEqualParameterNames(tableSchema.Columns, ", ", include);
            sql.AppendWherePrimaryKeysClause(tableSchema);
            return sql.ToString();
        }

        /// <summary>
        /// Generates a SQL Delete statement which chooses which row to delete its PrimaryKey.
        /// </summary>
        public static string MakeDeleteByPrimaryKeyStatement(TableSchema tableSchema)
        {
            var sql = new StringBuilder("DELETE FROM ").Append(tableSchema.Name);
            sql.AppendWherePrimaryKeysClause(tableSchema);
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

        private static void AppendWherePrimaryKeysClause(this StringBuilder sql, TableSchema tableSchema)
        {
            sql.AppendClause("WHERE ")
               .AppendColumnNamesEqualParameterNames(tableSchema.GetPrimaryKeys(), " AND ", p => true);
        }

        private static StringBuilder AppendSelectPropertiesClause(
            this StringBuilder sql,
            ImmutableArray<ColumnSchema> properties)
        {
            var isFirst = true;

            // ReSharper disable once ForCanBeConvertedToForeach
            // PERF: This method can be called in a very tight loop so should be as fast as possible
            for (var i = 0; i < properties.Length; i++)
            {
                var property = properties[i];

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

        /// <summary>
        /// Appends a list of properties in the form of ColumnName = @ParameterName {Seperator} ColumnName = @ParameterName ...
        /// </summary>
        private static StringBuilder AppendColumnNamesEqualParameterNames(
            this StringBuilder sql,
            ImmutableArray<ColumnSchema> properties,
            string seperator,
            Func<ColumnSchema, bool> include)
        {
            var isFirst = true;

            // ReSharper disable once ForCanBeConvertedToForeach
            // PERF: This method can be called in a very tight loop so should be as fast as possible
            for (var i = 0; i < properties.Length; i++)
            {
                var property = properties[i];
                if (!include(property))
                {
                    continue;
                }

                if (!isFirst)
                {
                    sql.Append(seperator);
                }

                sql.Append(property.ColumnName).Append(" = @").Append(property.ParameterName);
                isFirst = false;
            }

            return sql;
        }

        /// <summary>
        /// Appends a list of properties in the form of @ParameterName, @ParameterName ...
        /// </summary>
        private static StringBuilder AppendParameterNames(
            this StringBuilder sql,
            ImmutableArray<ColumnSchema> properties,
            Func<ColumnSchema, bool> include)
        {
            var isFirst = true;

            // ReSharper disable once ForCanBeConvertedToForeach
            // PERF: This method can be called in a very tight loop so should be as fast as possible
            for (var i = 0; i < properties.Length; i++)
            {
                var property = properties[i];
                if (!include(property))
                {
                    continue;
                }

                if (!isFirst)
                {
                    sql.Append(", ");
                }

                sql.Append("@").Append(property.ParameterName);
                isFirst = false;
            }

            return sql;
        }

        /// <summary>
        /// Appends a list of properties in the form of ColumnName, ColumnName ...
        /// </summary>
        private static StringBuilder AppendColumnNames(
            this StringBuilder sql,
            ImmutableArray<ColumnSchema> properties,
            Func<ColumnSchema, bool> include)
        {
            var isFirst = true;

            // ReSharper disable once ForCanBeConvertedToForeach
            // PERF: This method can be called in a very tight loop so should be as fast as possible
            for (var i = 0; i < properties.Length; i++)
            {
                var property = properties[i];

                if (!include(property))
                {
                    continue;
                }

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