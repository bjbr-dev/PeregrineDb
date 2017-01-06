// <copyright file="CommandFactory.cs" company="Berkeleybross">
// Copyright (c) Berkeleybross. All rights reserved.
// </copyright>
namespace Dapper.MicroCRUD
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Threading;
    using Dapper.MicroCRUD.Dialects;
    using Dapper.MicroCRUD.Schema;
    using Dapper.MicroCRUD.Utils;

    /// <summary>
    /// Creates <see cref="CommandDefinition"/>s to be executed.
    /// </summary>
    public static class CommandFactory
    {
        /// <summary>
        /// Creates a command which will count how many entities are in the table.
        /// </summary>
        public static CommandDefinition MakeCountCommand<TEntity>(
            string conditions,
            object parameters,
            IDbTransaction transaction,
            IDialect dialect,
            int? commandTimeout,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            dialect = dialect ?? MicroCRUDConfig.DefaultDialect;

            var tableSchema = TableSchemaFactory.GetTableSchema(typeof(TEntity), dialect);
            var sql = dialect.MakeCountStatement(tableSchema, conditions);
            return MakeCommandDefinition(sql, parameters, transaction, commandTimeout, cancellationToken);
        }

        /// <summary>
        /// Creates a command which will get an entity by its id
        /// </summary>
        public static CommandDefinition MakeFindCommand<TEntity>(
            object id,
            IDbTransaction transaction,
            IDialect dialect,
            int? commandTimeout,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            Ensure.NotNull(id, nameof(id));
            dialect = dialect ?? MicroCRUDConfig.DefaultDialect;

            var tableSchema = TableSchemaFactory.GetTableSchema(typeof(TEntity), dialect);
            var sql = dialect.MakeFindStatement(tableSchema);
            var parameters = tableSchema.GetPrimaryKeyParameters(id);
            return MakeCommandDefinition(sql, parameters, transaction, commandTimeout, cancellationToken);
        }

        /// <summary>
        /// Creates a command which will get all the entities matching the <paramref name="conditions"/>.
        /// </summary>
        public static CommandDefinition MakeGetRangeCommand<TEntity>(
            string conditions,
            object parameters,
            IDbTransaction transaction,
            IDialect dialect,
            int? commandTimeout,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            dialect = dialect ?? MicroCRUDConfig.DefaultDialect;

            var tableSchema = TableSchemaFactory.GetTableSchema(typeof(TEntity), dialect);
            var sql = dialect.MakeGetRangeStatement(tableSchema, conditions);
            return MakeCommandDefinition(sql, parameters, transaction, commandTimeout, cancellationToken);
        }

        /// <summary>
        /// Creates a command which will get a page of entities matching the <paramref name="conditions"/>.
        /// </summary>
        public static CommandDefinition MakeGetPageCommand<TEntity>(
            int pageNumber,
            int itemsPerPage,
            string conditions,
            string orderBy,
            object parameters,
            IDbTransaction transaction,
            IDialect dialect,
            int? commandTimeout,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            dialect = dialect ?? MicroCRUDConfig.DefaultDialect;

            var tableSchema = TableSchemaFactory.GetTableSchema(typeof(TEntity), dialect);
            var sql = dialect.MakeGetPageStatement(
                tableSchema, dialect, pageNumber, itemsPerPage, conditions, orderBy);
            return MakeCommandDefinition(sql, parameters, transaction, commandTimeout, cancellationToken);
        }

        /// <summary>
        /// Creates a command which will get all the entities
        /// </summary>
        public static CommandDefinition MakeGetAllCommand<TEntity>(
            IDbTransaction transaction,
            IDialect dialect,
            int? commandTimeout,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            dialect = dialect ?? MicroCRUDConfig.DefaultDialect;

            var tableSchema = TableSchemaFactory.GetTableSchema(typeof(TEntity), dialect);
            var sql = dialect.MakeGetRangeStatement(tableSchema, null);
            return MakeCommandDefinition(sql, null, transaction, commandTimeout, cancellationToken);
        }

        /// <summary>
        /// Creates a command which will insert an entity, not returning anything.
        /// </summary>
        public static CommandDefinition MakeInsertCommand(
            object entity,
            IDbTransaction transaction,
            IDialect dialect,
            int? commandTimeout,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            Ensure.NotNull(entity, nameof(entity));
            dialect = dialect ?? MicroCRUDConfig.DefaultDialect;

            var tableSchema = TableSchemaFactory.GetTableSchema(entity.GetType(), dialect);
            var sql = dialect.MakeInsertStatement(tableSchema);
            return MakeCommandDefinition(sql, entity, transaction, commandTimeout, cancellationToken);
        }

        /// <summary>
        /// Creates a command which will insert an entity, returning the primary key.
        /// </summary>
        public static CommandDefinition MakeInsertReturningPrimaryKeyCommand<TPrimaryKey>(
            object entity,
            IDbTransaction transaction,
            IDialect dialect,
            int? commandTimeout,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            Ensure.NotNull(entity, nameof(entity));
            dialect = dialect ?? MicroCRUDConfig.DefaultDialect;

            var tableSchema = TableSchemaFactory.GetTableSchema(entity.GetType(), dialect);

            if (!tableSchema.CanGeneratePrimaryKey(typeof(TPrimaryKey)))
            {
                throw new InvalidPrimaryKeyException(
                    "Insert<TPrimaryKey>() can only be used for Int32 and Int64 primary keys. Use Insert() for other types of primary keys.");
            }

            var sql = dialect.MakeInsertReturningIdentityStatement(tableSchema);
            return MakeCommandDefinition(sql, entity, transaction, commandTimeout, cancellationToken);
        }

        /// <summary>
        /// Creates a command which will insert a range of entities, not returning anything.
        /// </summary>
        public static CommandDefinition MakeInsertRangeCommand<TEntity>(
            IEnumerable<TEntity> entities,
            IDbTransaction transaction,
            IDialect dialect,
            int? commandTimeout,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            Ensure.NotNull(entities, nameof(entities));
            dialect = dialect ?? MicroCRUDConfig.DefaultDialect;

            var tableSchema = TableSchemaFactory.GetTableSchema(typeof(TEntity), dialect);
            var sql = dialect.MakeInsertStatement(tableSchema);
            return MakeCommandDefinition(sql, entities, transaction, commandTimeout, cancellationToken);
        }

        /// <summary>
        /// Creates a command factory which can be used to create entities for multiple inserts each returning the primary key.
        /// </summary>
        public static string MakeInsertRangeCommand<TEntity, TPrimaryKey>(IEnumerable<TEntity> entities, IDialect dialect)
        {
            Ensure.NotNull(entities, nameof(entities));
            dialect = dialect ?? MicroCRUDConfig.DefaultDialect;

            var tableSchema = TableSchemaFactory.GetTableSchema(typeof(TEntity), dialect);

            if (!tableSchema.CanGeneratePrimaryKey(typeof(TPrimaryKey)))
            {
                throw new InvalidPrimaryKeyException(
                    "InsertRange<TEntity, TPrimaryKey>() can only be used for Int32 and Int64 primary keys. Use InsertRange<TEntity>() for other types of primary keys.");
            }

            return dialect.MakeInsertReturningIdentityStatement(tableSchema);
        }

        /// <summary>
        /// Creates a command which will insert update an entity by using its primary key.
        /// </summary>
        public static CommandDefinition MakeUpdateCommand<TEntity>(
            object entity,
            IDbTransaction transaction,
            IDialect dialect,
            int? commandTimeout,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            Ensure.NotNull(entity, nameof(entity));
            dialect = dialect ?? MicroCRUDConfig.DefaultDialect;

            var tableSchema = TableSchemaFactory.GetTableSchema(typeof(TEntity), dialect);
            var sql = dialect.MakeUpdateStatement(tableSchema);
            return MakeCommandDefinition(sql, entity, transaction, commandTimeout, cancellationToken);
        }

        /// <summary>
        /// Creates a command which will update many entities
        /// </summary>
        public static CommandDefinition MakeUpdateRangeCommand<TEntity>(
            IEnumerable<TEntity> entities,
            IDbTransaction transaction,
            IDialect dialect,
            int? commandTimeout,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            Ensure.NotNull(entities, nameof(entities));
            dialect = dialect ?? MicroCRUDConfig.DefaultDialect;

            var tableSchema = TableSchemaFactory.GetTableSchema(typeof(TEntity), dialect);
            var sql = dialect.MakeUpdateStatement(tableSchema);
            return MakeCommandDefinition(sql, entities, transaction, commandTimeout, cancellationToken);
        }

        /// <summary>
        /// Creates a command which will delete an entity by usings it's primary key.
        /// </summary>
        public static CommandDefinition MakeDeleteCommand<TEntity>(
            object entity,
            IDbTransaction transaction,
            IDialect dialect,
            int? commandTimeout,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            Ensure.NotNull(entity, nameof(entity));
            dialect = dialect ?? MicroCRUDConfig.DefaultDialect;

            var tableSchema = TableSchemaFactory.GetTableSchema(typeof(TEntity), dialect);
            var sql = dialect.MakeDeleteByPrimaryKeyStatement(tableSchema);
            return MakeCommandDefinition(sql, entity, transaction, commandTimeout, cancellationToken);
        }

        /// <summary>
        /// Creates a command which will insert an entity, returning the primary key.
        /// </summary>
        public static CommandDefinition MakeDeleteByPrimaryKeyCommand<TEntity>(
            object id,
            IDbTransaction transaction,
            IDialect dialect,
            int? commandTimeout,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            Ensure.NotNull(id, nameof(id));
            dialect = dialect ?? MicroCRUDConfig.DefaultDialect;

            var tableSchema = TableSchemaFactory.GetTableSchema(typeof(TEntity), dialect);
            var sql = dialect.MakeDeleteByPrimaryKeyStatement(tableSchema);
            var parameters = tableSchema.GetPrimaryKeyParameters(id);
            return MakeCommandDefinition(sql, parameters, transaction, commandTimeout, cancellationToken);
        }

        /// <summary>
        /// Creates a command which will delete a range of entities, validating that the conditions contains a WHERE clause.
        /// </summary>
        public static CommandDefinition MakeDeleteRangeCommand<TEntity>(
            string conditions,
            object parameters,
            IDbTransaction transaction,
            IDialect dialect,
            int? commandTimeout,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            if (conditions == null || conditions.IndexOf("WHERE ", StringComparison.OrdinalIgnoreCase) < 0)
            {
                throw new ArgumentException(
                    "DeleteRange<TEntity> requires a WHERE clause, use DeleteAll<TEntity> to delete everything.");
            }

            dialect = dialect ?? MicroCRUDConfig.DefaultDialect;

            var tableSchema = TableSchemaFactory.GetTableSchema(typeof(TEntity), dialect);
            var sql = dialect.MakeDeleteRangeStatement(tableSchema, conditions);
            return MakeCommandDefinition(sql, parameters, transaction, commandTimeout, cancellationToken);
        }

        /// <summary>
        /// Creates a command which will delete all entities
        /// </summary>
        public static CommandDefinition MakeDeleteAllCommand<TEntity>(
            IDbTransaction transaction,
            IDialect dialect,
            int? commandTimeout,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            dialect = dialect ?? MicroCRUDConfig.DefaultDialect;

            var tableSchema = TableSchemaFactory.GetTableSchema(typeof(TEntity), dialect);
            var sql = dialect.MakeDeleteRangeStatement(tableSchema, null);
            return MakeCommandDefinition(sql, null, transaction, commandTimeout, cancellationToken);
        }

        private static CommandDefinition MakeCommandDefinition(
            string sql,
            object param,
            IDbTransaction transaction,
            int? commandTimeout,
            CancellationToken cancellationToken)
        {
            return new CommandDefinition(
                sql, param, transaction, commandTimeout, cancellationToken: cancellationToken);
        }
    }
}