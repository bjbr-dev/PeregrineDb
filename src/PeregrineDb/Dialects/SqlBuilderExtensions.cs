// <copyright file="SqlBuilderExtensions.cs" company="Berkeleybross">
// Copyright (c) Berkeleybross. All rights reserved.
// </copyright>

namespace PeregrineDb.Dialects
{
    using System;
    using System.Collections.Immutable;
    using PeregrineDb.Schema;

    /// <summary>
    /// Helpers to generate SQL statements.
    /// </summary>
    internal static class SqlBuilderExtensions
    {
        /// <summary>
        /// Appends a WHERE clause which selects equality of primary keys.
        /// </summary>
        public static SqlCommandBuilder AppendWherePrimaryKeysClause(this SqlCommandBuilder sql, ImmutableArray<ColumnSchema> primaryKeys)
        {
            return sql.AppendClause("WHERE ")
                      .AppendColumnNamesEqualParameters(primaryKeys, " AND ", p => true);
        }

        /// <summary>
        /// Appends a SQL clause which lists all the properties and their aliases.
        /// </summary>
        public static SqlCommandBuilder AppendSelectPropertiesClause(
            this SqlCommandBuilder sql,
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

                if (!string.Equals(property.ColumnName, property.SelectName, StringComparison.OrdinalIgnoreCase))
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
        public static SqlCommandBuilder AppendColumnNamesEqualParameters(
            this SqlCommandBuilder sql,
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
        public static SqlCommandBuilder AppendParameterNames(
            this SqlCommandBuilder sql,
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
        public static SqlCommandBuilder AppendColumnNames(
            this SqlCommandBuilder sql,
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
    }
}