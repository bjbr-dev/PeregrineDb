// <copyright file="DbConnectionExtensions.cs" company="Berkeleybross">
//   Copyright (c) Berkeleybross. All rights reserved.
// </copyright>
// ReSharper disable once CheckNamespace
namespace Dapper
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Linq;
    using Dapper.MicroCRUD;
    using Dapper.MicroCRUD.Entities;
    using Dapper.MicroCRUD.Utils;

    /// <summary>
    /// CRUD extensions to the <see cref="IDbConnection"/>.
    /// </summary>
    public static class DbConnectionExtensions
    {
        /// <summary>
        /// Counts how many entities in the <typeparamref name="TEntity"/> table match the <paramref name="conditions"/>.
        /// </summary>
        /// <example>
        /// <code>
        /// <![CDATA[
        /// [Table("Users")]
        /// public class UserEntity
        /// {
        ///     [Key]
        ///     public int Id { get; set; }
        ///
        ///     public string Name { get; set; }
        ///
        ///     public int Age { get; set; }
        /// }
        /// ...
        /// this.connection.Count<UserEntity>("WHERE Age > @MinAge", new { MinAge = 18 });
        /// ]]>
        /// </code>
        /// </example>
        public static int Count<TEntity>(
            this IDbConnection connection,
            string conditions = null,
            object parameters = null,
            IDbTransaction transaction = null,
            Dialect dialect = null,
            int? commandTimeout = null)
        {
            Ensure.NotNull(connection, nameof(connection));

            var tableSchema = TableSchemaCache.GetTableSchema(typeof(TEntity), dialect);
            var sql = SqlFactory.MakeCountStatement(tableSchema, conditions);
            return connection.ExecuteScalar<int>(sql, parameters, transaction, commandTimeout);
        }

        /// <summary>
        /// Finds a single entity from the <typeparamref name="TEntity"/> table by it's primary key, or the default value if not found.
        /// </summary>
        /// <example>
        /// <code>
        /// <![CDATA[
        /// [Table("Users")]
        /// public class UserEntity
        /// {
        ///     [Key]
        ///     public int Id { get; set; }
        ///
        ///     public string Name { get; set; }
        /// }
        /// ...
        /// var entity = this.connection.Find<UserEntity>(12);
        /// ]]>
        /// </code>
        /// </example>
        public static TEntity Find<TEntity>(
            this IDbConnection connection,
            object id,
            IDbTransaction transaction = null,
            Dialect dialect = null,
            int? commandTimeout = null)
        {
            Ensure.NotNull(connection, nameof(connection));
            Ensure.NotNull(id, nameof(id));

            var tableSchema = TableSchemaCache.GetTableSchema(typeof(TEntity), dialect);
            var sql = SqlFactory.MakeFindStatement(tableSchema);
            var parameters = GetPrimaryKeyParameters(tableSchema, id);
            return connection.Query<TEntity>(sql, parameters, transaction, commandTimeout: commandTimeout)
                             .FirstOrDefault();
        }

        /// <summary>
        /// Gets a collection of entities from the <typeparamref name="TEntity"/> table which match the <paramref name="conditions"/>.
        /// </summary>
        /// <example>
        /// <code>
        /// <![CDATA[
        /// [Table("Users")]
        /// public class UserEntity
        /// {
        ///     [Key]
        ///     public int Id { get; set; }
        ///
        ///     public string Name { get; set; }
        ///
        ///     public int Age { get; set; }
        /// }
        /// ...
        /// var users = this.connection.GetRange<UserEntity>("WHERE Age > @MinAge", new { MinAge = 18 });
        /// ]]>
        /// </code>
        /// </example>
        public static IEnumerable<TEntity> GetRange<TEntity>(
            this IDbConnection connection,
            string conditions,
            object parameters = null,
            IDbTransaction transaction = null,
            Dialect dialect = null,
            int? commandTimeout = null)
        {
            Ensure.NotNull(connection, nameof(connection));

            var tableSchema = TableSchemaCache.GetTableSchema(typeof(TEntity), dialect);
            var sql = SqlFactory.MakeGetRangeStatement(tableSchema, conditions);
            return connection.Query<TEntity>(sql, parameters, transaction, commandTimeout: commandTimeout);
        }

        /// <summary>
        /// Gets all the entities in the <typeparamref name="TEntity"/> table.
        /// </summary>
        public static IEnumerable<TEntity> GetAll<TEntity>(
            this IDbConnection connection,
            IDbTransaction transaction = null,
            Dialect dialect = null,
            int? commandTimeout = null)
        {
            Ensure.NotNull(connection, nameof(connection));

            var tableSchema = TableSchemaCache.GetTableSchema(typeof(TEntity), dialect);
            var sql = SqlFactory.MakeGetRangeStatement(tableSchema, null);
            return connection.Query<TEntity>(sql, transaction: transaction, commandTimeout: commandTimeout);
        }

        /// <summary>
        /// Inserts the <paramref name="entity"/> into the database.
        /// </summary>
        /// <example>
        /// <code>
        /// <![CDATA[
        /// [Table("Users")]
        /// public class UserEntity
        /// {
        ///     public string Name { get; set; }
        /// }
        /// ...
        /// var entity = new User { Name = "Little bobby tables" };
        /// this.connection.Insert(entity);
        /// ]]>
        /// </code>
        /// </example>
        public static void Insert(
            this IDbConnection connection,
            object entity,
            IDbTransaction transaction = null,
            Dialect dialect = null,
            int? commandTimeout = null)
        {
            Ensure.NotNull(connection, nameof(connection));
            Ensure.NotNull(entity, nameof(entity));

            var tableSchema = TableSchemaCache.GetTableSchema(entity.GetType(), dialect);
            var sql = SqlFactory.MakeInsertStatement(tableSchema);
            connection.Execute(sql, entity, transaction, commandTimeout);
        }

        /// <summary>
        /// Inserts the <paramref name="entity"/> into the database, and returns the auto-generated identity (or the default if invalid)
        /// </summary>
        /// <example>
        /// <code>
        /// <![CDATA[
        /// [Table("Users")]
        /// public class UserEntity
        /// {
        ///     [Key]
        ///     public int Id { get; set; }
        ///
        ///     public string Name { get; set; }
        /// }
        /// ...
        /// var entity = new User { Name = "Little bobby tables" };
        /// entity.Id = this.connection.Insert<int>(entity);
        /// ]]>
        /// </code>
        /// </example>
        public static TPrimaryKey Insert<TPrimaryKey>(
            this IDbConnection connection,
            object entity,
            IDbTransaction transaction = null,
            Dialect dialect = null,
            int? commandTimeout = null)
        {
            Ensure.NotNull(connection, nameof(connection));
            Ensure.NotNull(entity, nameof(entity));

            var config = MicroCRUDConfig.GetConfig(dialect);
            var tableSchema = TableSchemaCache.GetTableSchema(entity.GetType(), config);

            if (!tableSchema.CanGeneratePrimaryKey(typeof(TPrimaryKey)))
            {
                throw new InvalidPrimaryKeyException(
                    "Insert<TPrimaryKey>() can only be used for Int32 and Int64 primary keys. Use Insert() for other types of primary keys.");
            }

            var sql = SqlFactory.MakeInsertReturningIdentityStatement(tableSchema, config.Dialect);
            return connection.ExecuteScalar<TPrimaryKey>(sql, entity, transaction, commandTimeout);
        }

        /// <summary>
        /// <para>Efficiently inserts multiple <paramref name="entities"/> into the database.</para>
        /// <para>For performance, it's recommended to always perform this action inside of a transaction.</para>
        /// </summary>
        /// <example>
        /// <code>
        /// <![CDATA[
        /// [Table("Users")]
        /// public class UserEntity
        /// {
        ///     public string Name { get; set; }
        /// }
        /// ...
        /// var entities = new []
        ///     {
        ///         new User { Name = "Little bobby tables" },
        ///         new User { Name = "Jimmy" };
        ///     };
        ///
        /// using (var transaction = this.connection.BeginTransaction())
        /// {
        ///     this.connection.InsertRange(entities, transaction);
        ///
        ///     transaction.Commit();
        /// }
        /// ]]>
        /// </code>
        /// </example>
        public static void InsertRange<TEntity>(
            this IDbConnection connection,
            IEnumerable<TEntity> entities,
            IDbTransaction transaction = null,
            Dialect dialect = null,
            int? commandTimeout = null)
        {
            Ensure.NotNull(connection, nameof(connection));
            Ensure.NotNull(entities, nameof(entities));

            var tableSchema = TableSchemaCache.GetTableSchema(typeof(TEntity), dialect);
            var sql = SqlFactory.MakeInsertStatement(tableSchema);
            connection.Execute(sql, entities, transaction, commandTimeout);
        }

        /// <summary>
        /// <para>
        /// Efficiently inserts multiple <paramref name="entities"/> into the database,
        /// and for each one calls <paramref name="setPrimaryKey"/> allowing the primary key to be recorded.
        /// </para>
        /// <para>For performance, it's recommended to always perform this action inside of a transaction.</para>
        /// </summary>
        /// <example>
        /// <code>
        /// <![CDATA[
        /// [Table("Users")]
        /// public class UserEntity
        /// {
        ///     public int Id { get; set; }
        ///
        ///     public string Name { get; set; }
        /// }
        /// ...
        /// var entities = new []
        ///     {
        ///         new User { Name = "Little bobby tables" },
        ///         new User { Name = "Jimmy" };
        ///     };
        ///
        /// using (var transaction = this.connection.BeginTransaction())
        /// {
        ///     this.connection.InsertRange<User, int>(entities, (e, k) => { e.Id = k; }, transaction);
        ///
        ///     transaction.Commit();
        /// }
        /// ]]>
        /// </code>
        /// </example>
        public static void InsertRange<TEntity, TPrimaryKey>(
            this IDbConnection connection,
            IEnumerable<TEntity> entities,
            Action<TEntity, TPrimaryKey> setPrimaryKey,
            IDbTransaction transaction = null,
            Dialect dialect = null,
            int? commandTimeout = null)
        {
            Ensure.NotNull(connection, nameof(connection));
            Ensure.NotNull(entities, nameof(entities));
            Ensure.NotNull(setPrimaryKey, nameof(setPrimaryKey));

            var config = MicroCRUDConfig.GetConfig(dialect);
            var tableSchema = TableSchemaCache.GetTableSchema(typeof(TEntity), config);

            if (!tableSchema.CanGeneratePrimaryKey(typeof(TPrimaryKey)))
            {
                throw new InvalidPrimaryKeyException(
                    "InsertRange<TEntity, TPrimaryKey>() can only be used for Int32 and Int64 primary keys. Use InsertRange<TEntity>() for other types of primary keys.");
            }

            var sql = SqlFactory.MakeInsertReturningIdentityStatement(tableSchema, config.Dialect);
            foreach (var entity in entities)
            {
                var id = connection.ExecuteScalar<TPrimaryKey>(sql, entity, transaction, commandTimeout);
                setPrimaryKey(entity, id);
            }
        }

        /// <summary>
        /// Updates the <paramref name="entity"/> in the database.
        /// </summary>
        /// <example>
        /// <code>
        /// <![CDATA[
        /// [Table("Users")]
        /// public class UserEntity
        /// {
        ///     [Key]
        ///     public int Id { get; set; }
        ///
        ///     public string Name { get; set; }
        /// }
        /// ...
        /// var entity = this.connection.Find<UserEntity>(5);
        /// entity.Name = "Little bobby tables";
        /// this.connection.Update(entity);
        /// ]]>
        /// </code>
        /// </example>
        /// <returns>The number of affected records.</returns>
        public static int Update<TEntity>(
            this IDbConnection connection,
            TEntity entity,
            IDbTransaction transaction = null,
            Dialect dialect = null,
            int? commandTimeout = null)
        {
            Ensure.NotNull(connection, nameof(connection));

            // Shouldnt update a null entity, but entities *could* be a struct. Box into object (since Execute does that anyway) and ensure thats not null...
            var param = (object)entity;
            Ensure.NotNull(param, nameof(entity));

            var tableSchema = TableSchemaCache.GetTableSchema(typeof(TEntity), dialect);
            var sql = SqlFactory.MakeUpdateStatement(tableSchema);
            return connection.Execute(sql, param, transaction, commandTimeout);
        }

        /// <summary>
        /// Deletes the entity in the <typeparamref name="TEntity"/> table, identified by its primary key.
        /// </summary>
        /// <example>
        /// <code>
        /// <![CDATA[
        /// [Table("Users")]
        /// public class UserEntity
        /// {
        ///     [Key]
        ///     public int Id { get; set; }
        ///
        ///     public string Name { get; set; }
        /// }
        /// ...
        /// var entity = this.connection.Find<UserEntity>(5);
        /// this.connection.Delete(entity);
        /// ]]>
        /// </code>
        /// </example>
        /// <returns>The number of deleted entities.</returns>
        public static int Delete<TEntity>(
            this IDbConnection connection,
            TEntity entity,
            IDbTransaction transaction = null,
            Dialect dialect = null,
            int? commandTimeout = null)
        {
            Ensure.NotNull(connection, nameof(connection));

            // Shouldnt delete a null entity, but entities *could* be a struct. Box into object (since Execute does that anyway) and ensure thats not null...
            var param = (object)entity;
            Ensure.NotNull(param, nameof(entity));

            var tableSchema = TableSchemaCache.GetTableSchema(typeof(TEntity), dialect);
            var sql = SqlFactory.MakeDeleteByPrimaryKeyStatement(tableSchema);
            return connection.Execute(sql, param, transaction, commandTimeout);
        }

        /// <summary>
        /// Deletes the entity in the <typeparamref name="TEntity"/> table which has the <paramref name="id"/>.
        /// </summary>
        /// <example>
        /// <code>
        /// <![CDATA[
        /// [Table("Users")]
        /// public class UserEntity
        /// {
        ///     [Key]
        ///     public int Id { get; set; }
        ///
        ///     public string Name { get; set; }
        /// }
        /// ...
        /// this.connection.Delete(5);
        /// ]]>
        /// </code>
        /// </example>
        /// <returns>The number of deleted entities.</returns>
        public static int Delete<TEntity>(
            this IDbConnection connection,
            object id,
            IDbTransaction transaction = null,
            Dialect dialect = null,
            int? commandTimeout = null)
        {
            Ensure.NotNull(connection, nameof(connection));
            Ensure.NotNull(id, nameof(id));

            var tableSchema = TableSchemaCache.GetTableSchema(typeof(TEntity), dialect);
            var sql = SqlFactory.MakeDeleteByPrimaryKeyStatement(tableSchema);
            var parameters = GetPrimaryKeyParameters(tableSchema, id);
            return connection.Execute(sql, parameters, transaction, commandTimeout);
        }

        /// <summary>
        /// <para>Deletes all the entities in the <typeparamref name="TEntity"/> table which match the <paramref name="conditions"/>.</para>
        /// <para>Note: <paramref name="conditions"/> must contain a WHERE clause. Use <see cref="DeleteAll{TEntity}"/> if you want to delete all entities.</para>
        /// </summary>
        /// <example>
        /// <code>
        /// <![CDATA[
        /// [Table("Users")]
        /// public class UserEntity
        /// {
        ///     [Key]
        ///     public int Id { get; set; }
        ///
        ///     public string Name { get; set; }
        /// }
        /// ...
        /// this.connection.DeleteRange<UserEntity>("WHERE Name LIKE '%Foo%'");
        /// ]]>
        /// </code>
        /// </example>
        /// <returns>The number of deleted entities.</returns>
        public static int DeleteRange<TEntity>(
            this IDbConnection connection,
            string conditions,
            object parameters = null,
            IDbTransaction transaction = null,
            Dialect dialect = null,
            int? commandTimeout = null)
        {
            Ensure.NotNull(connection, nameof(connection));
            if (conditions == null || conditions.IndexOf("WHERE ", StringComparison.OrdinalIgnoreCase) < 0)
            {
                throw new ArgumentException(
                    "DeleteRange<TEntity> requires a WHERE clause, use DeleteAll<TEntity> to delete everything.");
            }

            var tableSchema = TableSchemaCache.GetTableSchema(typeof(TEntity), dialect);
            var sql = SqlFactory.MakeDeleteRangeStatement(tableSchema, conditions);
            return connection.Execute(sql, parameters, transaction, commandTimeout);
        }

        /// <summary>
        /// Deletes all the entities in the <typeparamref name="TEntity"/> table.
        /// </summary>
        /// <example>
        /// <code>
        /// <![CDATA[
        /// [Table("Users")]
        /// public class UserEntity
        /// {
        ///     [Key]
        ///     public int Id { get; set; }
        ///
        ///     public string Name { get; set; }
        /// }
        /// ...
        /// this.connection.DeleteAll<UserEntity>();
        /// ]]>
        /// </code>
        /// </example>
        /// <returns>The number of deleted entities.</returns>
        public static int DeleteAll<TEntity>(
            this IDbConnection connection,
            IDbTransaction transaction = null,
            Dialect dialect = null,
            int? commandTimeout = null)
        {
            Ensure.NotNull(connection, nameof(connection));

            var tableSchema = TableSchemaCache.GetTableSchema(typeof(TEntity), dialect);
            var sql = SqlFactory.MakeDeleteRangeStatement(tableSchema, null);
            return connection.Execute(sql, transaction: transaction, commandTimeout: commandTimeout);
        }

        private static object GetPrimaryKeyParameters(TableSchema tableSchema, object id)
        {
            var primaryKeys = tableSchema.GetPrimaryKeys();
            if (primaryKeys.Count > 1)
            {
                return id;
            }

            var parameters = new DynamicParameters();
            parameters.Add("@" + primaryKeys.First().ParameterName, id);
            return parameters;
        }
    }
}