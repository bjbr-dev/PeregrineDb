namespace PeregrineDb.SqlCommands
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Threading;
    using Dapper;
    using Pagination;
    using PeregrineDb.Schema;
    using PeregrineDb.Utils;

    /// <summary>
    /// Creates <see cref="CommandDefinition"/>s to be executed.
    /// </summary>
    internal class CommandFactory
    {
        private readonly PeregrineConfig config;
        private readonly IDbTransaction transaction;

        public CommandFactory(PeregrineConfig config, IDbTransaction transaction)
        {
            this.config = config ?? throw new ArgumentNullException(nameof(config));
            this.transaction = transaction;
        }

        /// <summary>
        /// Creates a command which will count how many entities are in the table.
        /// </summary>
        public CommandDefinition MakeCountCommand<TEntity>(string conditions, object parameters, int? timeout, CancellationToken cancellationToken = default)
        {
            var tableSchema = this.config.GetTableSchema(typeof(TEntity));
            var sql = this.config.Dialect.MakeCountStatement(tableSchema, conditions);
            return this.MakeCommandDefinition(sql, parameters, timeout, cancellationToken);
        }

        /// <summary>
        /// Creates a command which will count how many entities are in the table.
        /// </summary>
        public CommandDefinition MakeCountCommand<TEntity>(object conditions, int? timeout, CancellationToken cancellationToken = default)
        {
            Ensure.NotNull(conditions, nameof(conditions));

            var entityType = typeof(TEntity);
            var tableSchema = this.config.GetTableSchema(entityType);
            var conditionsSchema = this.config.GetConditionsSchema(entityType, tableSchema, conditions.GetType());
            var sql = this.config.Dialect.MakeCountStatement(tableSchema, this.config.Dialect.MakeWhereClause(conditionsSchema, conditions));
            return this.MakeCommandDefinition(sql, conditions, timeout, cancellationToken);
        }

        /// <summary>
        /// Creates a command which will get an entity by its id
        /// </summary>
        public CommandDefinition MakeFindCommand<TEntity>(object id,int? timeout,CancellationToken cancellationToken = default)
        {
            Ensure.NotNull(id, nameof(id));

            var tableSchema = this.config.GetTableSchema(typeof(TEntity));
            var sql = this.config.Dialect.MakeFindStatement(tableSchema);
            var parameters = tableSchema.GetPrimaryKeyParameters(id);
            return this.MakeCommandDefinition(sql, parameters, timeout, cancellationToken);
        }

        /// <summary>
        /// Creates a command which will get the top N entities which match the condition
        /// </summary>
        public CommandDefinition MakeGetTopNCommand<TEntity>(
            int take,
            string conditions,
            string orderBy,
            object parameters,
            int? timeout,
            CancellationToken cancellationToken = default)
        {
            var entityType = typeof(TEntity);
            var tableSchema = this.config.GetTableSchema(entityType);
            var sql = this.config.Dialect.MakeGetTopNStatement(tableSchema, take, conditions, orderBy);
            return this.MakeCommandDefinition(sql, parameters, timeout, cancellationToken);
        }

        /// <summary>
        /// Creates a command which will get the top N entities which match the condition
        /// </summary>
        public CommandDefinition MakeGetTopNCommand<TEntity>(
            int take,
            object conditions,
            string orderBy,
            int? timeout,
            CancellationToken cancellationToken = default)
        {
            Ensure.NotNull(conditions, nameof(conditions));

            var entityType = typeof(TEntity);
            var tableSchema = this.config.GetTableSchema(entityType);
            var conditionsSchema = this.config.GetConditionsSchema(entityType, tableSchema, conditions.GetType());
            var sql = this.config.Dialect.MakeGetTopNStatement(tableSchema, take, this.config.Dialect.MakeWhereClause(conditionsSchema, conditions), orderBy);
            return this.MakeCommandDefinition(sql, conditions, timeout, cancellationToken);
        }

        /// <summary>
        /// Creates a command which will get all the entities matching the <paramref name="conditions"/>.
        /// </summary>
        public CommandDefinition MakeGetRangeCommand<TEntity>(string conditions, object parameters, int? timeout, CancellationToken cancellationToken = default)
        {
            var tableSchema = this.config.GetTableSchema(typeof(TEntity));
            var sql = this.config.Dialect.MakeGetRangeStatement(tableSchema, conditions);
            return this.MakeCommandDefinition(sql, parameters, timeout, cancellationToken);
        }

        /// <summary>
        /// Creates a command which will get all the entities matching the <paramref name="conditions"/>.
        /// </summary>
        public CommandDefinition MakeGetRangeCommand<TEntity>(object conditions, int? timeout, CancellationToken cancellationToken = default)
        {
            Ensure.NotNull(conditions, nameof(conditions));

            var entityType = typeof(TEntity);
            var tableSchema = this.config.GetTableSchema(typeof(TEntity));
            var conditionsSchema = this.config.GetConditionsSchema(entityType, tableSchema, conditions.GetType());
            var sql = this.config.Dialect.MakeGetRangeStatement(tableSchema, this.config.Dialect.MakeWhereClause(conditionsSchema, conditions));
            return this.MakeCommandDefinition(sql, conditions, timeout, cancellationToken);
        }

        /// <summary>
        /// Creates a command which will get a page of entities matching the <paramref name="conditions"/>.
        /// </summary>
        public CommandDefinition MakeGetPageCommand<TEntity>(
            Page page,
            string conditions,
            string orderBy,
            object parameters,
            int? timeout,
            CancellationToken cancellationToken = default)
        {
            var tableSchema = this.config.GetTableSchema(typeof(TEntity));
            var sql = this.config.Dialect.MakeGetPageStatement(tableSchema, page, conditions, orderBy);
            return this.MakeCommandDefinition(sql, parameters, timeout, cancellationToken);
        }

        /// <summary>
        /// Creates a command which will get a page of entities matching the <paramref name="conditions"/>.
        /// </summary>
        public CommandDefinition MakeGetPageCommand<TEntity>(
            Page page,
            object conditions,
            string orderBy,
            int? timeout,
            CancellationToken cancellationToken = default)
        {
            Ensure.NotNull(conditions, nameof(conditions));

            var entityType = typeof(TEntity);
            var tableSchema = this.config.GetTableSchema(entityType);
            var conditionsSchema = this.config.GetConditionsSchema(entityType, tableSchema, conditions.GetType());
            var sql = this.config.Dialect.MakeGetPageStatement(tableSchema, page, this.config.Dialect.MakeWhereClause(conditionsSchema, conditions), orderBy);
            return this.MakeCommandDefinition(sql, conditions, timeout, cancellationToken);
        }

        /// <summary>
        /// Creates a command which will get all the entities
        /// </summary>
        public CommandDefinition MakeGetAllCommand<TEntity>(int? timeout, CancellationToken cancellationToken = default)
        {
            var tableSchema = this.config.GetTableSchema(typeof(TEntity));
            var sql = this.config.Dialect.MakeGetRangeStatement(tableSchema, null);
            return this.MakeCommandDefinition(sql, null, timeout, cancellationToken);
        }

        /// <summary>
        /// Creates a command which will insert an entity, not returning anything.
        /// </summary>
        public CommandDefinition MakeInsertCommand(object entity, int? timeout, CancellationToken cancellationToken = default)
        {
            Ensure.NotNull(entity, nameof(entity));

            var tableSchema = this.config.GetTableSchema(entity.GetType());
            var sql = this.config.Dialect.MakeInsertStatement(tableSchema);
            return this.MakeCommandDefinition(sql, entity, timeout, cancellationToken);
        }

        /// <summary>
        /// Creates a command which will insert an entity, returning the primary key.
        /// </summary>
        public CommandDefinition MakeInsertReturningPrimaryKeyCommand<TPrimaryKey>(object entity, int? timeout, CancellationToken cancellationToken = default)
        {
            Ensure.NotNull(entity, nameof(entity));

            var tableSchema = this.config.GetTableSchema(entity.GetType());

            if (!tableSchema.CanGeneratePrimaryKey(typeof(TPrimaryKey)))
            {
                throw new InvalidPrimaryKeyException(
                    "Insert<TPrimaryKey>() can only be used for Int32 and Int64 primary keys. Use Insert() for other types of primary keys.");
            }

            var sql = this.config.Dialect.MakeInsertReturningIdentityStatement(tableSchema);
            return this.MakeCommandDefinition(sql, entity, timeout, cancellationToken);
        }

        /// <summary>
        /// Creates a command which will insert a range of entities, not returning anything.
        /// </summary>
        public CommandDefinition MakeInsertRangeCommand<TEntity>(IEnumerable<TEntity> entities, int? timeout, CancellationToken cancellationToken = default)
        {
            Ensure.NotNull(entities, nameof(entities));

            var tableSchema = this.config.GetTableSchema(typeof(TEntity));
            var sql = this.config.Dialect.MakeInsertStatement(tableSchema);
            return this.MakeCommandDefinition(sql, entities, timeout, cancellationToken);
        }

        /// <summary>
        /// Creates a command factory which can be used to create entities for multiple inserts each returning the primary key.
        /// </summary>
        public string MakeInsertRangeCommand<TEntity, TPrimaryKey>()
        {
            var tableSchema = this.config.GetTableSchema(typeof(TEntity));

            if (!tableSchema.CanGeneratePrimaryKey(typeof(TPrimaryKey)))
            {
                throw new InvalidPrimaryKeyException(
                    "InsertRange<TEntity, TPrimaryKey>() can only be used for Int32 and Int64 primary keys. Use InsertRange<TEntity>() for other types of primary keys.");
            }

            return this.config.Dialect.MakeInsertReturningIdentityStatement(tableSchema);
        }

        /// <summary>
        /// Creates a command which will insert update an entity by using its primary key.
        /// </summary>
        public CommandDefinition MakeUpdateCommand<TEntity>(object entity,int? timeout,CancellationToken cancellationToken = default)
        {
            Ensure.NotNull(entity, nameof(entity));

            var tableSchema = this.config.GetTableSchema(typeof(TEntity));
            var sql = this.config.Dialect.MakeUpdateStatement(tableSchema);
            return this.MakeCommandDefinition(sql, entity, timeout, cancellationToken);
        }

        /// <summary>
        /// Creates a command which will update many entities
        /// </summary>
        public CommandDefinition MakeUpdateRangeCommand<TEntity>(IEnumerable<TEntity> entities, int? timeout, CancellationToken cancellationToken = default)
        {
            Ensure.NotNull(entities, nameof(entities));

            var tableSchema = this.config.GetTableSchema(typeof(TEntity));
            var sql = this.config.Dialect.MakeUpdateStatement(tableSchema);
            return this.MakeCommandDefinition(sql, entities, timeout, cancellationToken);
        }

        /// <summary>
        /// Creates a command which will delete an entity by usings it's primary key.
        /// </summary>
        public CommandDefinition MakeDeleteCommand<TEntity>(object entity, int? timeout, CancellationToken cancellationToken = default)
        {
            Ensure.NotNull(entity, nameof(entity));

            var tableSchema = this.config.GetTableSchema(typeof(TEntity));
            var sql = this.config.Dialect.MakeDeleteByPrimaryKeyStatement(tableSchema);
            return this.MakeCommandDefinition(sql, entity, timeout, cancellationToken);
        }

        /// <summary>
        /// Creates a command which will insert an entity, returning the primary key.
        /// </summary>
        public CommandDefinition MakeDeleteByPrimaryKeyCommand<TEntity>(object id, int? timeout, CancellationToken cancellationToken = default)
        {
            Ensure.NotNull(id, nameof(id));

            var tableSchema = this.config.GetTableSchema(typeof(TEntity));
            var sql = this.config.Dialect.MakeDeleteByPrimaryKeyStatement(tableSchema);
            var parameters = tableSchema.GetPrimaryKeyParameters(id);
            return this.MakeCommandDefinition(sql, parameters, timeout, cancellationToken);
        }

        /// <summary>
        /// Creates a command which will delete a range of entities, validating that the conditions contains a WHERE clause.
        /// </summary>
        public CommandDefinition MakeDeleteRangeCommand<TEntity>(
            string conditions,
            object parameters,
            int? timeout,
            CancellationToken cancellationToken = default)
        {
            if (conditions == null || conditions.IndexOf("WHERE ", StringComparison.OrdinalIgnoreCase) < 0)
            {
                throw new ArgumentException(
                    "DeleteRange<TEntity> requires a WHERE clause, use DeleteAll<TEntity> to delete everything.");
            }

            var tableSchema = this.config.GetTableSchema(typeof(TEntity));
            var sql = this.config.Dialect.MakeDeleteRangeStatement(tableSchema, conditions);
            return this.MakeCommandDefinition(sql, parameters, timeout, cancellationToken);
        }

        /// <summary>
        /// Creates a command which will delete a range of entities, validating that the conditions has at least one property
        /// </summary>
        public CommandDefinition MakeDeleteRangeCommand<TEntity>(object conditions, int? timeout, CancellationToken cancellationToken = default)
        {
            Ensure.NotNull(conditions, nameof(conditions));

            var entityType = typeof(TEntity);
            var tableSchema = this.config.GetTableSchema(entityType);
            var conditionsSchema = this.config.GetConditionsSchema(entityType, tableSchema, conditions.GetType());
            if (conditionsSchema.IsEmpty)
            {
                throw new ArgumentException("DeleteRange<TEntity> requires at least one condition, use DeleteAll<TEntity> to delete everything.");
            }

            var sql = this.config.Dialect.MakeDeleteRangeStatement(tableSchema, this.config.Dialect.MakeWhereClause(conditionsSchema, conditions));
            return this.MakeCommandDefinition(sql, conditions, timeout, cancellationToken);
        }

        /// <summary>
        /// Creates a command which will delete all entities
        /// </summary>
        public CommandDefinition MakeDeleteAllCommand<TEntity>(int? timeout, CancellationToken cancellationToken = default)
        {
            var tableSchema = this.config.GetTableSchema(typeof(TEntity));
            var sql = this.config.Dialect.MakeDeleteRangeStatement(tableSchema, null);
            return this.MakeCommandDefinition(sql, null, timeout, cancellationToken);
        }

        /// <summary>
        /// Creates a command which will delete all entities
        /// </summary>
        public CommandDefinition MakeCreateTempTableCommand<TEntity>(int? timeout, CancellationToken cancellationToken = default)
        {
            var tableSchema = this.config.GetTableSchema(typeof(TEntity));
            var sql = this.config.Dialect.MakeCreateTempTableStatement(tableSchema);
            return this.MakeCommandDefinition(sql, null, timeout, cancellationToken);
        }

        /// <summary>
        /// Creates a command which will delete all entities
        /// </summary>
        public CommandDefinition MakeDropTempTableCommand<TEntity>(string tableName, int? timeout, CancellationToken cancellationToken = default)
        {
            var tableSchema = this.config.GetTableSchema(typeof(TEntity));
            if (tableSchema.Name != tableName)
            {
                throw new ArgumentException($"Attempting to drop table '{tableSchema.Name}', but said table name should be '{tableName}'");
            }

            var sql = this.config.Dialect.MakeDropTempTableStatement(tableSchema);
            return this.MakeCommandDefinition(sql, null, timeout, cancellationToken);
        }

        private CommandDefinition MakeCommandDefinition(string sql, object param, int? commandTimeout, CancellationToken cancellationToken)
        {
            return new CommandDefinition(sql, param, this.transaction, commandTimeout, cancellationToken: cancellationToken);
        }
    }
}