// <copyright file="StandardDialect.cs" company="Berkeleybross">
// Copyright (c) Berkeleybross. All rights reserved.
// </copyright>

namespace PeregrineDb.Dialects
{
    using System;
    using System.Collections.Generic;
    using System.Collections.Immutable;
    using System.Text;
    using Pagination;
    using PeregrineDb.Schema;
    using PeregrineDb.Utils;

    /// <summary>
    /// Simple implementation of a SQL dialect which performs most SQL generation.
    /// </summary>
    public abstract class StandardDialect
        : IDialect, IEquatable<StandardDialect>
    {
        /// <inheritdoc />
        public SqlCommand MakeCountCommand(string conditions, object parameters, TableSchema tableSchema)
        {
            var sql = new SqlCommandBuilder("SELECT COUNT(*)");
            sql.AppendClause("FROM ").Append(tableSchema.Name);
            sql.AppendClause(conditions);
            return sql.ToCommand(parameters);
        }

        /// <inheritdoc />
        public SqlCommand MakeFindCommand(object id, TableSchema tableSchema)
        {
            Ensure.NotNull(id, nameof(id));
            var primaryKeys = tableSchema.GetPrimaryKeys();

            var sql = new SqlCommandBuilder("SELECT ").AppendSelectPropertiesClause(tableSchema.Columns);
            sql.AppendClause("FROM ").Append(tableSchema.Name);
            sql.AppendWherePrimaryKeysClause(primaryKeys);
            return sql.ToPrimaryKeySql(tableSchema, id);
        }

        /// <inheritdoc />
        public abstract SqlCommand MakeGetFirstNCommand(int take, string conditions, object parameters, string orderBy, TableSchema tableSchema);

        /// <inheritdoc />
        public SqlCommand MakeGetRangeCommand(string conditions, object parameters, TableSchema tableSchema)
        {
            var sql = new SqlCommandBuilder("SELECT ").AppendSelectPropertiesClause(tableSchema.Columns);
            sql.AppendClause("FROM ").Append(tableSchema.Name);
            sql.AppendClause(conditions);
            return sql.ToCommand(parameters);
        }

        /// <inheritdoc />
        public abstract SqlCommand MakeGetPageCommand(Page page, string conditions, object parameters, string orderBy, TableSchema tableSchema);

        /// <inheritdoc />
        public SqlCommand MakeInsertCommand(object entity, TableSchema tableSchema)
        {
            bool Include(ColumnSchema p) => p.Usage.IncludeInInsertStatements;
            var columns = tableSchema.Columns;

            var sql = new SqlCommandBuilder("INSERT INTO ").Append(tableSchema.Name).Append(" (").AppendColumnNames(columns, Include).Append(")");
            sql.AppendClause("VALUES (").AppendParameterNames(columns, Include).Append(");");
            return sql.ToCommand(entity);
        }

        /// <inheritdoc />
        public abstract SqlCommand MakeInsertReturningPrimaryKeyCommand(object entity, TableSchema tableSchema);

        /// <inheritdoc />
        public SqlMultipleCommand<TEntity> MakeInsertRangeCommand<TEntity>(IEnumerable<TEntity> entities, TableSchema tableSchema)
        {
            bool Include(ColumnSchema p) => p.Usage.IncludeInInsertStatements;
            var columns = tableSchema.Columns;

            var sql = new SqlCommandBuilder("INSERT INTO ").Append(tableSchema.Name).Append(" (").AppendColumnNames(columns, Include).Append(")");
            sql.AppendClause("VALUES (").AppendParameterNames(columns, Include).Append(");");
            return sql.ToMultipleCommand(entities);
        }

        /// <inheritdoc />
        public SqlCommand MakeUpdateCommand(object entity, TableSchema tableSchema)
        {
            Ensure.NotNull(entity, nameof(entity));

            bool Include(ColumnSchema p) => p.Usage.IncludeInUpdateStatements;

            var sql = new SqlCommandBuilder("UPDATE ").Append(tableSchema.Name);
            sql.AppendClause("SET ").AppendColumnNamesEqualParameters(tableSchema.Columns, ", ", Include);
            sql.AppendWherePrimaryKeysClause(tableSchema.GetPrimaryKeys());
            return sql.ToCommand(entity);
        }

        public SqlMultipleCommand<TEntity> MakeUpdateRangeCommand<TEntity>(IEnumerable<TEntity> entities, TableSchema tableSchema)
            where TEntity : class
        {
            bool Include(ColumnSchema p) => p.Usage.IncludeInUpdateStatements;

            var sql = new SqlCommandBuilder("UPDATE ").Append(tableSchema.Name);
            sql.AppendClause("SET ").AppendColumnNamesEqualParameters(tableSchema.Columns, ", ", Include);
            sql.AppendWherePrimaryKeysClause(tableSchema.GetPrimaryKeys());
            return sql.ToMultipleCommand(entities);
        }

        /// <inheritdoc />
        public SqlCommand MakeDeleteCommand(object entity, TableSchema tableSchema)
        {
            Ensure.NotNull(entity, nameof(entity));

            var sql = new SqlCommandBuilder("DELETE FROM ").Append(tableSchema.Name);
            sql.AppendWherePrimaryKeysClause(tableSchema.GetPrimaryKeys());
            return sql.ToCommand(entity);
        }

        /// <inheritdoc />
        public SqlCommand MakeDeleteByPrimaryKeyCommand(object id, TableSchema tableSchema)
        {
            var sql = new SqlCommandBuilder("DELETE FROM ").Append(tableSchema.Name);
            var primaryKeys = tableSchema.GetPrimaryKeys();
            sql.AppendWherePrimaryKeysClause(primaryKeys);
            return sql.ToPrimaryKeySql(tableSchema, id);
        }

        /// <summary>
        /// Generates a SQL Delete statement which chooses which row to delete by the <paramref name="conditions"/>.
        /// </summary>
        public SqlCommand MakeDeleteRangeCommand(string conditions, object parameters, TableSchema tableSchema)
        {
            if (conditions == null || conditions.IndexOf("WHERE ", StringComparison.OrdinalIgnoreCase) < 0)
            {
                throw new ArgumentException(
                    "DeleteRange<TEntity> requires a WHERE clause, use DeleteAll<TEntity> to delete everything.");
            }

            var sql = new SqlCommandBuilder("DELETE FROM ").Append(tableSchema.Name);
            sql.AppendClause(conditions);
            return sql.ToCommand(parameters);
        }

        public SqlCommand MakeDeleteAllCommand(TableSchema tableSchema)
        {
            return new SqlCommand("DELETE FROM " + tableSchema.Name);
        }

        public string MakeWhereClause(ImmutableArray<ConditionColumnSchema> conditionsSchema, object conditions)
        {
            if (conditionsSchema.IsEmpty)
            {
                return string.Empty;
            }

            var sql = new StringBuilder("WHERE ");
            var isFirst = true;

            foreach (var condition in conditionsSchema)
            {
                if (!isFirst)
                {
                    sql.Append(" AND ");
                }

                if (condition.IsNull(conditions))
                {
                    sql.Append(condition.Column.ColumnName).Append(" IS NULL");
                }
                else
                {
                    sql.Append(condition.Column.ColumnName).Append(" = @").Append(condition.Column.ParameterName);
                }

                isFirst = false;
            }

            return sql.ToString();
        }

        /// <inheritdoc />
        public abstract SqlCommand MakeCreateTempTableCommand(TableSchema tableSchema);

        /// <inheritdoc />
        public abstract SqlCommand MakeDropTempTableCommand(TableSchema tableSchema);

        public override bool Equals(object obj)
        {
            return this.Equals(obj as StandardDialect);
        }

        public bool Equals(StandardDialect other)
        {
            if (ReferenceEquals(null, other))
            {
                return false;
            }

            if (ReferenceEquals(this, other))
            {
                return true;
            }

            return this.GetType() == other.GetType();
        }

        public override int GetHashCode()
        {
            return this.GetType().Name.GetHashCode();
        }
    }
}