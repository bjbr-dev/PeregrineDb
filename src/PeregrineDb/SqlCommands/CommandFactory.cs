namespace PeregrineDb.SqlCommands
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using Pagination;
    using PeregrineDb.Schema;
    using PeregrineDb.Utils;

    /// <summary>
    /// Creates <see cref="SqlCommand"/>s to be executed.
    /// </summary>
    internal class CommandFactory
    {
        private readonly PeregrineConfig config;

        public CommandFactory(PeregrineConfig config)
        {
            this.config = config ?? throw new ArgumentNullException(nameof(config));
        }

        /// <summary>
        /// Creates a command which will count how many entities are in the table.
        /// </summary>
        public SqlCommand MakeCountCommand<TEntity>(FormattableString conditions)
        {
            var tableSchema = this.config.GetTableSchema(typeof(TEntity));
            return this.config.Dialect.MakeCountCommand(tableSchema, conditions);
        }

        /// <summary>
        /// Creates a command which will count how many entities are in the table.
        /// </summary>
        public SqlCommand MakeCountCommand<TEntity>(object conditions)
        {
            Ensure.NotNull(conditions, nameof(conditions));

            var entityType = typeof(TEntity);
            var tableSchema = this.config.GetTableSchema(entityType);
            var conditionsSchema = this.config.GetConditionsSchema(entityType, tableSchema, conditions.GetType());
            return this.config.Dialect.MakeCountCommand(tableSchema, this.config.Dialect.MakeWhereClause(conditionsSchema, conditions));
        }

        /// <summary>
        /// Creates a command which will get an entity by its id
        /// </summary>
        public SqlCommand MakeFindCommand<TEntity>(object id)
        {
            Ensure.NotNull(id, nameof(id));

            var tableSchema = this.config.GetTableSchema(typeof(TEntity));
            return this.config.Dialect.MakeFindCommand(tableSchema, id);
        }

        /// <summary>
        /// Creates a command which will get the top N entities which match the condition
        /// </summary>
        public SqlCommand MakeGetTopNCommand<TEntity>(int take, FormattableString conditions, string orderBy)
        {
            Ensure.NotNull(conditions, nameof(conditions));

            var entityType = typeof(TEntity);
            var tableSchema = this.config.GetTableSchema(entityType);
            return this.config.Dialect.MakeGetTopNCommand(tableSchema, take, conditions, orderBy);
        }

        /// <summary>
        /// Creates a command which will get the top N entities which match the condition
        /// </summary>
        public SqlCommand MakeGetTopNCommand<TEntity>(int take, object conditions, string orderBy)
        {

            Ensure.NotNull(conditions, nameof(conditions));

            var entityType = typeof(TEntity);
            var tableSchema = this.config.GetTableSchema(entityType);
            var conditionsSchema = this.config.GetConditionsSchema(entityType, tableSchema, conditions.GetType());
            var whereClause = this.config.Dialect.MakeWhereClause(conditionsSchema, conditions);
            return this.config.Dialect.MakeGetTopNCommand(tableSchema, take, whereClause, orderBy);
        }

        /// <summary>
        /// Creates a command which will get all the entities matching the <paramref name="conditions"/>.
        /// </summary>
        public SqlCommand MakeGetRangeCommand<TEntity>(FormattableString conditions)
        {
            var tableSchema = this.config.GetTableSchema(typeof(TEntity));
            return this.config.Dialect.MakeGetRangeCommand(tableSchema, conditions);
        }

        /// <summary>
        /// Creates a command which will get all the entities matching the <paramref name="conditions"/>.
        /// </summary>
        public SqlCommand MakeGetRangeCommand<TEntity>(object conditions)
        {
            Ensure.NotNull(conditions, nameof(conditions));

            var entityType = typeof(TEntity);
            var tableSchema = this.config.GetTableSchema(typeof(TEntity));
            var conditionsSchema = this.config.GetConditionsSchema(entityType, tableSchema, conditions.GetType());
            return this.config.Dialect.MakeGetRangeCommand(tableSchema, this.config.Dialect.MakeWhereClause(conditionsSchema, conditions));
        }

        /// <summary>
        /// Creates a command which will get a page of entities matching the <paramref name="conditions"/>.
        /// </summary>
        public SqlCommand MakeGetPageCommand<TEntity>(Page page, FormattableString conditions, string orderBy)
        {
            var tableSchema = this.config.GetTableSchema(typeof(TEntity));
            return this.config.Dialect.MakeGetPageCommand(tableSchema, page, conditions, orderBy);
        }

        /// <summary>
        /// Creates a command which will get a page of entities matching the <paramref name="conditions"/>.
        /// </summary>
        public SqlCommand MakeGetPageCommand<TEntity>(Page page, object conditions, string orderBy)
        {
            Ensure.NotNull(conditions, nameof(conditions));

            var entityType = typeof(TEntity);
            var tableSchema = this.config.GetTableSchema(entityType);
            var conditionsSchema = this.config.GetConditionsSchema(entityType, tableSchema, conditions.GetType());
            return this.config.Dialect.MakeGetPageCommand(tableSchema, page, this.config.Dialect.MakeWhereClause(conditionsSchema, conditions), orderBy);
        }

        /// <summary>
        /// Creates a command which will get all the entities
        /// </summary>
        public SqlCommand MakeGetAllCommand<TEntity>()
        {
            var tableSchema = this.config.GetTableSchema(typeof(TEntity));
            return this.config.Dialect.MakeGetRangeCommand(tableSchema, null);
        }

        /// <summary>
        /// Creates a command which will insert an entity, not returning anything.
        /// </summary>
        public SqlCommand MakeInsertCommand(object entity)
        {
            Ensure.NotNull(entity, nameof(entity));

            var tableSchema = this.config.GetTableSchema(entity.GetType());
            return this.config.Dialect.MakeInsertCommand(tableSchema, entity);
        }

        /// <summary>
        /// Creates a command which will insert an entity, returning the primary key.
        /// </summary>
        public SqlCommand MakeInsertReturningPrimaryKeyCommand<TPrimaryKey>(object entity)
        {
            Ensure.NotNull(entity, nameof(entity));

            var tableSchema = this.config.GetTableSchema(entity.GetType());

            if (!tableSchema.CanGeneratePrimaryKey(typeof(TPrimaryKey)))
            {
                throw new InvalidPrimaryKeyException(
                    "Insert<TPrimaryKey>() can only be used for Int32 and Int64 primary keys. Use Insert() for other types of primary keys.");
            }

            return this.config.Dialect.MakeInsertReturningIdentityCommand(tableSchema, entity);
        }

        /// <summary>
        /// Creates a command which will insert a range of entities, not returning anything.
        /// </summary>
        public SqlCommand MakeInsertRangeCommand<TEntity>(IEnumerable<TEntity> entities)
        {
            Ensure.NotNull(entities, nameof(entities));

            var tableSchema = this.config.GetTableSchema(typeof(TEntity));
            return this.config.Dialect.MakeInsertCommand(tableSchema, entities.First());
        }

        /// <summary>
        /// Creates a command factory which can be used to create entities for multiple inserts each returning the primary key.
        /// </summary>
        public SqlCommand MakeInsertRangeCommand<TEntity, TPrimaryKey>(IEnumerable<TEntity> entities)
        {
            var tableSchema = this.config.GetTableSchema(typeof(TEntity));

            if (!tableSchema.CanGeneratePrimaryKey(typeof(TPrimaryKey)))
            {
                throw new InvalidPrimaryKeyException(
                    "InsertRange<TEntity, TPrimaryKey>() can only be used for Int32 and Int64 primary keys. Use InsertRange<TEntity>() for other types of primary keys.");
            }

            return this.config.Dialect.MakeInsertReturningIdentityCommand(tableSchema, entities.First());
        }

        /// <summary>
        /// Creates a command which will insert update an entity by using its primary key.
        /// </summary>
        public SqlCommand MakeUpdateCommand<TEntity>(object entity)
        {
            Ensure.NotNull(entity, nameof(entity));

            var tableSchema = this.config.GetTableSchema(typeof(TEntity));
            return this.config.Dialect.MakeUpdateCommand(tableSchema, entity);
        }

        /// <summary>
        /// Creates a command which will update many entities
        /// </summary>
        public SqlCommand MakeUpdateRangeCommand<TEntity>(IEnumerable<TEntity> entities)
        {
            Ensure.NotNull(entities, nameof(entities));

            var tableSchema = this.config.GetTableSchema(typeof(TEntity));
            return this.config.Dialect.MakeUpdateCommand(tableSchema, entities.First());
        }

        /// <summary>
        /// Creates a command which will delete an entity by usings it's primary key.
        /// </summary>
        public SqlCommand MakeDeleteCommand<TEntity>(object entity)
        {
            Ensure.NotNull(entity, nameof(entity));

            var tableSchema = this.config.GetTableSchema(typeof(TEntity));
            return this.config.Dialect.MakeDeleteEntityCommand(tableSchema, entity);
        }

        /// <summary>
        /// Creates a command which will insert an entity, returning the primary key.
        /// </summary>
        public SqlCommand MakeDeleteByPrimaryKeyCommand<TEntity>(object id)
        {
            Ensure.NotNull(id, nameof(id));

            var tableSchema = this.config.GetTableSchema(typeof(TEntity));
            return this.config.Dialect.MakeDeleteByPrimaryKeyCommand(tableSchema, id);
        }

        /// <summary>
        /// Creates a command which will delete a range of entities, validating that the conditions contains a WHERE clause.
        /// </summary>
        public SqlCommand MakeDeleteRangeCommand<TEntity>(FormattableString conditions)
        {
            if (conditions == null || conditions.Format.IndexOf("WHERE ", StringComparison.OrdinalIgnoreCase) < 0)
            {
                throw new ArgumentException(
                    "DeleteRange<TEntity> requires a WHERE clause, use DeleteAll<TEntity> to delete everything.");
            }

            var tableSchema = this.config.GetTableSchema(typeof(TEntity));
            return this.config.Dialect.MakeDeleteRangeCommand(tableSchema, conditions);
        }

        /// <summary>
        /// Creates a command which will delete a range of entities, validating that the conditions has at least one property
        /// </summary>
        public SqlCommand MakeDeleteRangeCommand<TEntity>(object conditions)
        {
            Ensure.NotNull(conditions, nameof(conditions));

            var entityType = typeof(TEntity);
            var tableSchema = this.config.GetTableSchema(entityType);
            var conditionsSchema = this.config.GetConditionsSchema(entityType, tableSchema, conditions.GetType());
            if (conditionsSchema.IsEmpty)
            {
                throw new ArgumentException("DeleteRange<TEntity> requires at least one condition, use DeleteAll<TEntity> to delete everything.");
            }

            return this.config.Dialect.MakeDeleteRangeCommand(tableSchema, this.config.Dialect.MakeWhereClause(conditionsSchema, conditions));
        }

        /// <summary>
        /// Creates a command which will delete all entities
        /// </summary>
        public SqlCommand MakeDeleteAllCommand<TEntity>()
        {
            var tableSchema = this.config.GetTableSchema(typeof(TEntity));
            return this.config.Dialect.MakeDeleteRangeCommand(tableSchema, null);
        }

        /// <summary>
        /// Creates a command which will delete all entities
        /// </summary>
        public SqlCommand MakeCreateTempTableCommand<TEntity>()
        {
            var tableSchema = this.config.GetTableSchema(typeof(TEntity));
            return this.config.Dialect.MakeCreateTempTableCommand(tableSchema);
        }

        /// <summary>
        /// Creates a command which will delete all entities
        /// </summary>
        public SqlCommand MakeDropTempTableCommand<TEntity>(string tableName)
        {
            var tableSchema = this.config.GetTableSchema(typeof(TEntity));
            if (tableSchema.Name != tableName)
            {
                throw new ArgumentException($"Attempting to drop table '{tableSchema.Name}', but said table name should be '{tableName}'");
            }

            return this.config.Dialect.MakeDropTempTableCommand(tableSchema);
        }
    }
}