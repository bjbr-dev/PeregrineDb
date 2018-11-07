// <copyright file="CommandFactory.cs" company="Berkeleybross">
// Copyright (c) Berkeleybross. All rights reserved.
// </copyright>
namespace PeregrineDb.Databases
{
    using System;
    using System.Collections.Generic;
    using System.Collections.Immutable;
    using Pagination;
    using PeregrineDb.Dialects;
    using PeregrineDb.Schema;
    using PeregrineDb.Utils;

    internal sealed class CommandFactory
    {
        public CommandFactory(PeregrineConfig config)
        {
            this.Config = config;
        }

        private IDialect Dialect => this.Config.Dialect;

        public PeregrineConfig Config { get; }

        public SqlCommand MakeCountCommand<TEntity>(string conditions, object parameters)
        {
            return this.Dialect.MakeCountCommand(conditions, parameters, this.GetTableSchema(typeof(TEntity)));
        }

        public SqlCommand MakeCountCommand<TEntity>(object conditions)
        {
            Ensure.NotNull(conditions, nameof(conditions));

            var entityType = typeof(TEntity);
            var tableSchema = this.GetTableSchema(entityType);
            var conditionsSchema = this.GetConditionsSchema(entityType, tableSchema, conditions.GetType());
            var whereClause = this.Dialect.MakeWhereClause(conditionsSchema, conditions);

            return this.Dialect.MakeCountCommand(whereClause, conditions, tableSchema);
        }

        public SqlCommand MakeFindCommand<TEntity>(object id)
        {
            return this.Dialect.MakeFindCommand(id, this.GetTableSchema(typeof(TEntity)));
        }

        public SqlCommand MakeGetFirstNCommand<TEntity>(int take, string conditions, object parameters, string orderBy)
        {
            return this.Dialect.MakeGetFirstNCommand(take, conditions, parameters, orderBy, this.GetTableSchema(typeof(TEntity)));
        }

        public SqlCommand MakeGetFirstNCommand<TEntity>(int take, object conditions, string orderBy)
        {
            Ensure.NotNull(conditions, nameof(conditions));

            var entityType = typeof(TEntity);
            var tableSchema = this.GetTableSchema(entityType);
            var conditionsSchema = this.GetConditionsSchema(entityType, tableSchema, conditions.GetType());
            var whereClause = this.Dialect.MakeWhereClause(conditionsSchema, conditions);

            return this.Dialect.MakeGetFirstNCommand(take, whereClause, conditions, orderBy, tableSchema);
        }

        public SqlCommand MakeGetRangeCommand<TEntity>(string conditions, object parameters)
        {
            return this.Dialect.MakeGetRangeCommand(conditions, parameters, this.GetTableSchema(typeof(TEntity)));
        }

        public SqlCommand MakeGetRangeCommand<TEntity>(object conditions)
        {
            Ensure.NotNull(conditions, nameof(conditions));

            var entityType = typeof(TEntity);
            var tableSchema = this.GetTableSchema(entityType);
            var conditionsSchema = this.GetConditionsSchema(entityType, tableSchema, conditions.GetType());
            var whereClause = this.Dialect.MakeWhereClause(conditionsSchema, conditions);

            return this.Dialect.MakeGetRangeCommand(whereClause, conditions, tableSchema);
        }

        public SqlCommand MakeGetPageCommand<TEntity>(Page page, string conditions, object parameters, string orderBy)
        {
            return this.Dialect.MakeGetPageCommand(page, conditions, parameters, orderBy, this.GetTableSchema(typeof(TEntity)));
        }

        public SqlCommand MakeGetPageCommand<TEntity>(Page page, object conditions, string orderBy)
        {
            Ensure.NotNull(conditions, nameof(conditions));

            var entityType = typeof(TEntity);
            var tableSchema = this.GetTableSchema(entityType);
            var conditionsSchema = this.GetConditionsSchema(entityType, tableSchema, conditions.GetType());
            var whereClause = this.Dialect.MakeWhereClause(conditionsSchema, conditions);

            return this.Dialect.MakeGetPageCommand(page, whereClause, conditions, orderBy, tableSchema);
        }

        public SqlCommand MakeInsertCommand(object entity)
        {
            Ensure.NotNull(entity, nameof(entity));

            return this.Dialect.MakeInsertCommand(entity, this.GetTableSchema(entity.GetType()));
        }

        public SqlCommand MakeInsertReturningPrimaryKeyCommand<TPrimaryKey>(object entity)
        {
            var tableSchema = this.GetTableSchema(entity.GetType());

            if (!tableSchema.CanGeneratePrimaryKey(typeof(TPrimaryKey)))
            {
                throw new InvalidPrimaryKeyException(
                    "Insert<TPrimaryKey>() can only be used for Int32 and Int64 primary keys. Use Insert() for other types of primary keys.");
            }

            return this.Dialect.MakeInsertReturningPrimaryKeyCommand(entity, tableSchema);
        }

        public SqlMultipleCommand<TEntity> MakeInsertRangeCommand<TEntity>(IEnumerable<TEntity> entities)
        {
            return this.Dialect.MakeInsertRangeCommand(entities, this.GetTableSchema(typeof(TEntity)));
        }

        public SqlCommand MakeUpdateCommand<TEntity>(TEntity entity)
            where TEntity : class
        {
            return this.Dialect.MakeUpdateCommand(entity, this.GetTableSchema(typeof(TEntity)));
        }

        public SqlMultipleCommand<TEntity> MakeUpdateRangeCommand<TEntity>(IEnumerable<TEntity> entities)
            where TEntity : class
        {
            return this.Dialect.MakeUpdateRangeCommand(entities, this.GetTableSchema(typeof(TEntity)));
        }

        public SqlCommand MakeDeleteCommand<TEntity>(TEntity entity)
            where TEntity : class
        {
            return this.Dialect.MakeDeleteCommand(entity, this.GetTableSchema(typeof(TEntity)));
        }

        public SqlCommand MakeDeleteByPrimaryKeyCommand<TEntity>(object id)
        {
            return this.Dialect.MakeDeleteByPrimaryKeyCommand(id, this.GetTableSchema(typeof(TEntity)));
        }

        public SqlCommand MakeDeleteRangeCommand<TEntity>(string conditions, object parameters)
        {
            return this.Dialect.MakeDeleteRangeCommand(conditions, parameters, this.GetTableSchema(typeof(TEntity)));
        }

        public SqlCommand MakeDeleteRangeCommand<TEntity>(object conditions)
        {
            Ensure.NotNull(conditions, nameof(conditions));

            var entityType = typeof(TEntity);
            var tableSchema = this.GetTableSchema(entityType);
            var conditionsSchema = this.GetConditionsSchema(entityType, tableSchema, conditions.GetType());
            var whereClause = this.Dialect.MakeWhereClause(conditionsSchema, conditions);

            return this.Dialect.MakeDeleteRangeCommand(whereClause, conditions, tableSchema);
        }

        public SqlCommand MakeDeleteAllCommand<TEntity>()
        {
            return this.Dialect.MakeDeleteAllCommand(this.GetTableSchema(typeof(TEntity)));
        }

        public SqlCommand MakeCreateTempTableCommand<TEntity>()
        {
            return this.Dialect.MakeCreateTempTableCommand(this.GetTableSchema(typeof(TEntity)));
        }

        public SqlCommand MakeDropTempTableCommand<TEntity>()
        {
            return this.Dialect.MakeDropTempTableCommand(this.GetTableSchema(typeof(TEntity)));
        }

        private TableSchema GetTableSchema(Type entityType)
        {
            return this.Config.SchemaFactory.GetTableSchema(entityType);
        }

        private ImmutableArray<ConditionColumnSchema> GetConditionsSchema(
            Type entityType,
            TableSchema tableSchema,
            Type conditionsType)
        {
            return this.Config.SchemaFactory.GetConditionsSchema(entityType, tableSchema, conditionsType);
        }
    }
}
