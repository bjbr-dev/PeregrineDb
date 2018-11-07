// <copyright file="StandardDialect.cs" company="Berkeleybross">
// Copyright (c) Berkeleybross. All rights reserved.
// </copyright>

namespace PeregrineDb.Dialects
{
    using System;
    using System.Collections;
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
        private readonly TableSchemaFactory tableSchemaFactory;

        protected StandardDialect(TableSchemaFactory tableSchemaFactory)
        {
            this.tableSchemaFactory = tableSchemaFactory;
        }

        /// <inheritdoc />
        public SqlCommand MakeCountCommand<TEntity>(string conditions, object parameters)
        {
            var schema = this.GetTableSchema(typeof(TEntity));

            var sql = new SqlCommandBuilder("SELECT COUNT(*)");
            sql.AppendClause("FROM ").Append(schema.Name);
            sql.AppendClause(conditions);
            return sql.ToCommand(parameters);
        }

        /// <inheritdoc />
        public SqlCommand MakeCountCommand<TEntity>(object conditions)
        {
            Ensure.NotNull(conditions, nameof(conditions));

            var entityType = typeof(TEntity);
            var tableSchema = this.GetTableSchema(entityType);
            var conditionsSchema = this.GetConditionsSchema(entityType, tableSchema, conditions.GetType());

            var sql = new SqlCommandBuilder("SELECT COUNT(*)");
            sql.AppendClause("FROM ").Append(tableSchema.Name);
            sql.AppendClause(this.MakeWhereClause(conditionsSchema, conditions));
            return sql.ToCommand(conditions);
        }

        /// <inheritdoc />
        public SqlCommand MakeFindCommand<TEntity>(object id)
        {
            Ensure.NotNull(id, nameof(id));
            var schema = this.GetTableSchema(typeof(TEntity));
            var primaryKeys = schema.GetPrimaryKeys();

            var sql = new SqlCommandBuilder("SELECT ").AppendSelectPropertiesClause(schema.Columns);
            sql.AppendClause("FROM ").Append(schema.Name);
            sql.AppendWherePrimaryKeysClause(primaryKeys);
            return sql.ToPrimaryKeySql(schema, id);
        }

        /// <inheritdoc />
        public abstract SqlCommand MakeGetFirstNCommand(int take, string conditions, object parameters, string orderBy, TableSchema tableSchema);

        /// <inheritdoc />
        public abstract SqlCommand MakeGetFirstNCommand<TEntity>(int take, object conditions, string orderBy);

        /// <inheritdoc />
        public SqlCommand MakeGetRangeCommand<TEntity>(string conditions, object parameters)
        {
            var tableSchema = this.GetTableSchema(typeof(TEntity));

            var sql = new SqlCommandBuilder("SELECT ").AppendSelectPropertiesClause(tableSchema.Columns);
            sql.AppendClause("FROM ").Append(tableSchema.Name);
            sql.AppendClause(conditions);
            return sql.ToCommand(parameters);
        }

        /// <inheritdoc />
        public SqlCommand MakeGetRangeCommand<TEntity>(object conditions)
        {
            Ensure.NotNull(conditions, nameof(conditions));

            var entityType = typeof(TEntity);
            var tableSchema = this.GetTableSchema(typeof(TEntity));
            var conditionsSchema = this.GetConditionsSchema(entityType, tableSchema, conditions.GetType());

            var sql = new SqlCommandBuilder("SELECT ").AppendSelectPropertiesClause(tableSchema.Columns);
            sql.AppendClause("FROM ").Append(tableSchema.Name);
            sql.AppendClause(this.MakeWhereClause(conditionsSchema, conditions));
            return sql.ToCommand(conditions);
        }

        /// <inheritdoc />
        public abstract SqlCommand MakeGetPageCommand<TEntity>(Page page, string conditions, object parameters, string orderBy);

        /// <inheritdoc />
        public abstract SqlCommand MakeGetPageCommand<TEntity>(Page page, object conditions, string orderBy);

        /// <inheritdoc />
        public SqlCommand MakeInsertCommand(object entity)
        {
            Ensure.NotNull(entity, nameof(entity));

            var tableSchema = this.GetTableSchema(entity.GetType());
            bool Include(ColumnSchema p) => p.Usage.IncludeInInsertStatements;
            var columns = tableSchema.Columns;

            var sql = new SqlCommandBuilder("INSERT INTO ").Append(tableSchema.Name).Append(" (").AppendColumnNames(columns, Include).Append(")");
            sql.AppendClause("VALUES (").AppendParameterNames(columns, Include).Append(");");
            return sql.ToCommand(entity);
        }

        /// <inheritdoc />
        public abstract SqlCommand MakeInsertReturningPrimaryKeyCommand<TPrimaryKey>(object entity, TableSchema tableSchema);

        /// <inheritdoc />
        public SqlMultipleCommand<TEntity> MakeInsertRangeCommand<TEntity>(IEnumerable<TEntity> entities)
        {
            var tableSchema = this.GetTableSchema(typeof(TEntity));

            bool Include(ColumnSchema p) => p.Usage.IncludeInInsertStatements;
            var columns = tableSchema.Columns;

            var sql = new SqlCommandBuilder("INSERT INTO ").Append(tableSchema.Name).Append(" (").AppendColumnNames(columns, Include).Append(")");
            sql.AppendClause("VALUES (").AppendParameterNames(columns, Include).Append(");");
            return sql.ToMultipleCommand(entities);
        }

        /// <inheritdoc />
        public SqlCommand MakeUpdateCommand<TEntity>(TEntity entity)
            where TEntity : class
        {
            Ensure.NotNull(entity, nameof(entity));

            var tableSchema = this.GetTableSchema(typeof(TEntity));

            bool Include(ColumnSchema p) => p.Usage.IncludeInUpdateStatements;

            var sql = new SqlCommandBuilder("UPDATE ").Append(tableSchema.Name);
            sql.AppendClause("SET ").AppendColumnNamesEqualParameters(tableSchema.Columns, ", ", Include);
            sql.AppendWherePrimaryKeysClause(tableSchema.GetPrimaryKeys());
            return sql.ToCommand(entity);
        }

        public SqlMultipleCommand<TEntity> MakeUpdateRangeCommand<TEntity>(IEnumerable<TEntity> entities)
            where TEntity : class
        {
            var tableSchema = this.GetTableSchema(typeof(TEntity));

            bool Include(ColumnSchema p) => p.Usage.IncludeInUpdateStatements;

            var sql = new SqlCommandBuilder("UPDATE ").Append(tableSchema.Name);
            sql.AppendClause("SET ").AppendColumnNamesEqualParameters(tableSchema.Columns, ", ", Include);
            sql.AppendWherePrimaryKeysClause(tableSchema.GetPrimaryKeys());
            return sql.ToMultipleCommand(entities);
        }

        /// <inheritdoc />
        public SqlCommand MakeDeleteCommand<TEntity>(TEntity entity)
            where TEntity : class
        {
            Ensure.NotNull(entity, nameof(entity));

            var tableSchema = this.GetTableSchema(typeof(TEntity));

            var sql = new SqlCommandBuilder("DELETE FROM ").Append(tableSchema.Name);
            sql.AppendWherePrimaryKeysClause(tableSchema.GetPrimaryKeys());
            return sql.ToCommand(entity);
        }

        /// <inheritdoc />
        public SqlCommand MakeDeleteByPrimaryKeyCommand<TEntity>(object id)
        {
            var schema = this.GetTableSchema(typeof(TEntity));

            var sql = new SqlCommandBuilder("DELETE FROM ").Append(schema.Name);
            var primaryKeys = schema.GetPrimaryKeys();
            sql.AppendWherePrimaryKeysClause(primaryKeys);
            return sql.ToPrimaryKeySql(schema, id);
        }

        /// <summary>
        /// Generates a SQL Delete statement which chooses which row to delete by the <paramref name="conditions"/>.
        /// </summary>
        public SqlCommand MakeDeleteRangeCommand<TEntity>(string conditions, object parameters)
        {
            if (conditions == null || conditions.IndexOf("WHERE ", StringComparison.OrdinalIgnoreCase) < 0)
            {
                throw new ArgumentException(
                    "DeleteRange<TEntity> requires a WHERE clause, use DeleteAll<TEntity> to delete everything.");
            }

            var tableSchema = this.GetTableSchema(typeof(TEntity));

            var sql = new SqlCommandBuilder("DELETE FROM ").Append(tableSchema.Name);
            sql.AppendClause(conditions);
            return sql.ToCommand(parameters);
        }

        public SqlCommand MakeDeleteAllCommand<TEntity>()
        {
            var tableSchema = this.GetTableSchema(typeof(TEntity));

            return new SqlCommand("DELETE FROM " + tableSchema.Name);
        }

        /// <summary>
        /// Generates a SQL Delete statement which chooses which row to delete by the <paramref name="conditions"/>.
        /// </summary>
        public SqlCommand MakeDeleteRangeCommand<TEntity>(object conditions)
        {
            Ensure.NotNull(conditions, nameof(conditions));

            var entityType = typeof(TEntity);
            var tableSchema = this.GetTableSchema(entityType);
            var conditionsSchema = this.GetConditionsSchema(entityType, tableSchema, conditions.GetType());
            if (conditionsSchema.IsEmpty)
            {
                throw new ArgumentException("DeleteRange<TEntity> requires at least one condition, use DeleteAll<TEntity> to delete everything.");
            }

            var sql = new SqlCommandBuilder("DELETE FROM ").Append(tableSchema.Name);
            sql.AppendClause(this.MakeWhereClause(conditionsSchema, conditions));
            return sql.ToCommand(conditions);
        }

        protected string MakeWhereClause(ImmutableArray<ConditionColumnSchema> conditionsSchema, object conditions)
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
        public abstract SqlCommand MakeCreateTempTableCommand<TEntity>();

        /// <inheritdoc />
        public abstract SqlCommand MakeDropTempTableCommand<TEntity>();

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

        protected TableSchema GetTableSchema(Type entityType)
        {
            return this.tableSchemaFactory.GetTableSchema(entityType);
        }

        protected ImmutableArray<ConditionColumnSchema> GetConditionsSchema(
            Type entityType,
            TableSchema tableSchema,
            Type conditionsType)
        {
            return this.tableSchemaFactory.GetConditionsSchema(entityType, tableSchema, conditionsType);
        }
    }
}