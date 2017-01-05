// <copyright file="DbConnectionAsyncExtensions.cs" company="Berkeleybross">
//   Copyright (c) Berkeleybross. All rights reserved.
// </copyright>
// ReSharper disable once CheckNamespace
namespace Dapper
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Diagnostics.CodeAnalysis;
    using System.Linq;
    using System.Threading.Tasks;
    using Dapper.MicroCRUD;
    using Dapper.MicroCRUD.Dialects;
    using Dapper.MicroCRUD.Schema;
    using Dapper.MicroCRUD.Utils;

    /// <summary>
    /// Async CRUD extensions to the <see cref="IDbConnection"/>.
    /// </summary>
    public static class DbConnectionAsyncExtensions
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
        public static Task<int> CountAsync<TEntity>(
            this IDbConnection connection,
            string conditions = null,
            object parameters = null,
            IDbTransaction transaction = null,
            IDialect dialect = null,
            int? commandTimeout = null)
        {
            Ensure.NotNull(connection, nameof(connection));
            dialect = dialect ?? MicroCRUDConfig.DefaultDialect;

            var tableSchema = TableSchemaFactory.GetTableSchema(typeof(TEntity), dialect);
            var sql = dialect.MakeCountStatement(tableSchema, conditions);
            return connection.ExecuteScalarAsync<int>(sql, parameters, transaction, commandTimeout);
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
        public static async Task<TEntity> FindAsync<TEntity>(
            this IDbConnection connection,
            object id,
            IDbTransaction transaction = null,
            IDialect dialect = null,
            int? commandTimeout = null)
        {
            Ensure.NotNull(connection, nameof(connection));
            Ensure.NotNull(id, nameof(id));
            dialect = dialect ?? MicroCRUDConfig.DefaultDialect;

            var tableSchema = TableSchemaFactory.GetTableSchema(typeof(TEntity), dialect);
            var sql = dialect.MakeFindStatement(tableSchema);
            var parameters = tableSchema.GetPrimaryKeyParameters(id);
            var result = await connection.QueryAsync<TEntity>(sql, parameters, transaction, commandTimeout)
                                         .ConfigureAwait(false);
            return result.FirstOrDefault();
        }

        /// <summary>
        /// Gets a single entity from the <typeparamref name="TEntity"/> table by it's primary key, or throws an exception if not found
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
        /// var entity = this.connection.Get<UserEntity>(12);
        /// ]]>
        /// </code>
        /// </example>
        /// <exception cref="InvalidOperationException">The entity was not found.</exception>
        public static async Task<TEntity> GetAsync<TEntity>(
            this IDbConnection connection,
            object id,
            IDbTransaction transaction = null,
            IDialect dialect = null,
            int? commandTimeout = null)
            where TEntity : class
        {
            var result = await connection.FindAsync<TEntity>(id, transaction, dialect, commandTimeout)
                                         .ConfigureAwait(false);
            if (result != null)
            {
                return result;
            }

            throw new InvalidOperationException($"An entity with id {id} was not found");
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
        public static Task<IEnumerable<TEntity>> GetRangeAsync<TEntity>(
            this IDbConnection connection,
            string conditions,
            object parameters = null,
            IDbTransaction transaction = null,
            IDialect dialect = null,
            int? commandTimeout = null)
        {
            Ensure.NotNull(connection, nameof(connection));
            dialect = dialect ?? MicroCRUDConfig.DefaultDialect;

            var tableSchema = TableSchemaFactory.GetTableSchema(typeof(TEntity), dialect);
            var sql = dialect.MakeGetRangeStatement(tableSchema, conditions);
            return connection.QueryAsync<TEntity>(sql, parameters, transaction, commandTimeout);
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
        /// var users = this.connection.GetPage<UserEntity>(3, 10, "WHERE Age > @MinAge", "Age DESC", new { MinAge = 18 });
        /// ]]>
        /// </code>
        /// </example>
        public static Task<IEnumerable<TEntity>> GetPageAsync<TEntity>(
            this IDbConnection connection,
            int pageNumber,
            int itemsPerPage,
            string conditions,
            string orderBy,
            object parameters = null,
            IDbTransaction transaction = null,
            IDialect dialect = null,
            int? commandTimeout = null)
        {
            Ensure.NotNull(connection, nameof(connection));
            dialect = dialect ?? MicroCRUDConfig.DefaultDialect;

            var tableSchema = TableSchemaFactory.GetTableSchema(typeof(TEntity), dialect);
            var sql = dialect.MakeGetPageStatement(
                tableSchema, dialect, pageNumber, itemsPerPage, conditions, orderBy);

            return connection.QueryAsync<TEntity>(sql, parameters, transaction, commandTimeout);
        }

        /// <summary>
        /// Gets all the entities in the <typeparamref name="TEntity"/> table.
        /// </summary>
        public static Task<IEnumerable<TEntity>> GetAllAsync<TEntity>(
            this IDbConnection connection,
            IDbTransaction transaction = null,
            IDialect dialect = null,
            int? commandTimeout = null)
        {
            Ensure.NotNull(connection, nameof(connection));
            dialect = dialect ?? MicroCRUDConfig.DefaultDialect;

            var tableSchema = TableSchemaFactory.GetTableSchema(typeof(TEntity), dialect);
            var sql = dialect.MakeGetRangeStatement(tableSchema, null);
            return connection.QueryAsync<TEntity>(sql, transaction: transaction, commandTimeout: commandTimeout);
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
        public static async Task InsertAsync(
            this IDbConnection connection,
            object entity,
            IDbTransaction transaction = null,
            IDialect dialect = null,
            int? commandTimeout = null)
        {
            Ensure.NotNull(connection, nameof(connection));
            Ensure.NotNull(entity, nameof(entity));
            dialect = dialect ?? MicroCRUDConfig.DefaultDialect;

            var tableSchema = TableSchemaFactory.GetTableSchema(entity.GetType(), dialect);
            var sql = dialect.MakeInsertStatement(tableSchema);
            var result = await connection.ExecuteCommandAsync(sql, entity, transaction, commandTimeout)
                                         .ConfigureAwait(false);
            result.ExpectingAffectedRowCountToBe(1);
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
        public static Task<TPrimaryKey> InsertAsync<TPrimaryKey>(
            this IDbConnection connection,
            object entity,
            IDbTransaction transaction = null,
            IDialect dialect = null,
            int? commandTimeout = null)
        {
            Ensure.NotNull(connection, nameof(connection));
            Ensure.NotNull(entity, nameof(entity));
            dialect = dialect ?? MicroCRUDConfig.DefaultDialect;

            var tableSchema = TableSchemaFactory.GetTableSchema(entity.GetType(), dialect);

            if (!tableSchema.CanGeneratePrimaryKey(typeof(TPrimaryKey)))
            {
                throw new InvalidPrimaryKeyException(
                    "Insert<TPrimaryKey>() can only be used for Int32 and Int64 primary keys. Use Insert() for other types of primary keys.");
            }

            var sql = dialect.MakeInsertReturningIdentityStatement(tableSchema);
            return connection.ExecuteScalarAsync<TPrimaryKey>(sql, entity, transaction, commandTimeout);
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
        public static Task<SqlCommandResult> InsertRangeAsync<TEntity>(
            this IDbConnection connection,
            IEnumerable<TEntity> entities,
            IDbTransaction transaction = null,
            IDialect dialect = null,
            int? commandTimeout = null)
        {
            Ensure.NotNull(connection, nameof(connection));
            Ensure.NotNull(entities, nameof(entities));
            dialect = dialect ?? MicroCRUDConfig.DefaultDialect;

            var tableSchema = TableSchemaFactory.GetTableSchema(typeof(TEntity), dialect);
            var sql = dialect.MakeInsertStatement(tableSchema);
            return connection.ExecuteCommandAsync(sql, entities, transaction, commandTimeout);
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
        [SuppressMessage("ReSharper", "PossibleMultipleEnumeration", Justification = "Ensure doesn't enumerate")]
        public static async Task InsertRangeAsync<TEntity, TPrimaryKey>(
            this IDbConnection connection,
            IEnumerable<TEntity> entities,
            Action<TEntity, TPrimaryKey> setPrimaryKey,
            IDbTransaction transaction = null,
            IDialect dialect = null,
            int? commandTimeout = null)
        {
            Ensure.NotNull(connection, nameof(connection));
            Ensure.NotNull(entities, nameof(entities));
            Ensure.NotNull(setPrimaryKey, nameof(setPrimaryKey));
            dialect = dialect ?? MicroCRUDConfig.DefaultDialect;

            var tableSchema = TableSchemaFactory.GetTableSchema(typeof(TEntity), dialect);

            if (!tableSchema.CanGeneratePrimaryKey(typeof(TPrimaryKey)))
            {
                throw new InvalidPrimaryKeyException(
                    "InsertRange<TEntity, TPrimaryKey>() can only be used for Int32 and Int64 primary keys. Use InsertRange<TEntity>() for other types of primary keys.");
            }

            var sql = dialect.MakeInsertReturningIdentityStatement(tableSchema);
            foreach (var entity in entities)
            {
                var id = await connection.ExecuteScalarAsync<TPrimaryKey>(sql, entity, transaction, commandTimeout)
                                         .ConfigureAwait(false);
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
        /// <exception cref="AffectedRowCountException">The update command didn't change any record, or changed multiple records.</exception>
        public static async Task UpdateAsync<TEntity>(
            this IDbConnection connection,
            TEntity entity,
            IDbTransaction transaction = null,
            IDialect dialect = null,
            int? commandTimeout = null)
        {
            Ensure.NotNull(connection, nameof(connection));

            // Shouldnt update a null entity, but entities *could* be a struct. Box into object (since Execute does that anyway) and ensure thats not null...
            var param = (object)entity;
            Ensure.NotNull(param, nameof(entity));
            dialect = dialect ?? MicroCRUDConfig.DefaultDialect;

            var tableSchema = TableSchemaFactory.GetTableSchema(typeof(TEntity), dialect);
            var sql = dialect.MakeUpdateStatement(tableSchema);
            var result = await connection.ExecuteCommandAsync(sql, param, transaction, commandTimeout)
                                         .ConfigureAwait(false);
            result.ExpectingAffectedRowCountToBe(1);
        }

        /// <summary>
        /// Efficiently updates multiple <paramref name="entities"/> in the database.
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
        /// using (var transaction = this.connection.BeginTransaction())
        /// {
        ///     var entities = this.connection.GetRange<UserEntity>("WHERE @Age = 10");
        ///
        ///     foreach (var entity in entities)
        ///     {
        ///         entity.Name = "Little bobby tables";
        ///     }
        ///
        ///     this.connection.UpdateRange(entities);
        ///     transaction.Commit();
        /// }
        /// ]]>
        /// </code>
        /// </example>
        /// <returns>The number of affected records.</returns>
        public static Task<SqlCommandResult> UpdateRangeAsync<TEntity>(
            this IDbConnection connection,
            IEnumerable<TEntity> entities,
            IDbTransaction transaction = null,
            IDialect dialect = null,
            int? commandTimeout = null)
        {
            Ensure.NotNull(connection, nameof(connection));
            Ensure.NotNull(entities, nameof(entities));
            dialect = dialect ?? MicroCRUDConfig.DefaultDialect;

            var tableSchema = TableSchemaFactory.GetTableSchema(typeof(TEntity), dialect);
            var sql = dialect.MakeUpdateStatement(tableSchema);
            return connection.ExecuteCommandAsync(sql, entities, transaction, commandTimeout);
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
        /// <exception cref="AffectedRowCountException">The delete command didn't delete anything, or deleted multiple records.</exception>
        public static async Task DeleteAsync<TEntity>(
            this IDbConnection connection,
            TEntity entity,
            IDbTransaction transaction = null,
            IDialect dialect = null,
            int? commandTimeout = null)
        {
            Ensure.NotNull(connection, nameof(connection));

            // Shouldnt delete a null entity, but entities *could* be a struct. Box into object (since Execute does that anyway) and ensure thats not null...
            var param = (object)entity;
            Ensure.NotNull(param, nameof(entity));
            dialect = dialect ?? MicroCRUDConfig.DefaultDialect;

            var tableSchema = TableSchemaFactory.GetTableSchema(typeof(TEntity), dialect);
            var sql = dialect.MakeDeleteByPrimaryKeyStatement(tableSchema);
            var result = await connection.ExecuteCommandAsync(sql, param, transaction, commandTimeout)
                                         .ConfigureAwait(false);
            result.ExpectingAffectedRowCountToBe(1);
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
        /// <exception cref="AffectedRowCountException">The delete command didn't delete anything, or deleted multiple records.</exception>
        public static async Task DeleteAsync<TEntity>(
            this IDbConnection connection,
            object id,
            IDbTransaction transaction = null,
            IDialect dialect = null,
            int? commandTimeout = null)
        {
            Ensure.NotNull(connection, nameof(connection));
            Ensure.NotNull(id, nameof(id));
            dialect = dialect ?? MicroCRUDConfig.DefaultDialect;

            var tableSchema = TableSchemaFactory.GetTableSchema(typeof(TEntity), dialect);
            var sql = dialect.MakeDeleteByPrimaryKeyStatement(tableSchema);
            var parameters = tableSchema.GetPrimaryKeyParameters(id);
            var result = await connection.ExecuteCommandAsync(sql, parameters, transaction, commandTimeout)
                                         .ConfigureAwait(false);
            result.ExpectingAffectedRowCountToBe(1);
        }

        /// <summary>
        /// <para>Deletes all the entities in the <typeparamref name="TEntity"/> table which match the <paramref name="conditions"/>.</para>
        /// <para>Note: <paramref name="conditions"/> must contain a WHERE clause. Use <see cref="DeleteAllAsync{TEntity}"/> if you want to delete all entities.</para>
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
        public static Task<SqlCommandResult> DeleteRangeAsync<TEntity>(
            this IDbConnection connection,
            string conditions,
            object parameters = null,
            IDbTransaction transaction = null,
            IDialect dialect = null,
            int? commandTimeout = null)
        {
            Ensure.NotNull(connection, nameof(connection));
            if (conditions == null || conditions.IndexOf("WHERE ", StringComparison.OrdinalIgnoreCase) < 0)
            {
                throw new ArgumentException(
                    "DeleteRange<TEntity> requires a WHERE clause, use DeleteAll<TEntity> to delete everything.");
            }

            dialect = dialect ?? MicroCRUDConfig.DefaultDialect;

            var tableSchema = TableSchemaFactory.GetTableSchema(typeof(TEntity), dialect);
            var sql = dialect.MakeDeleteRangeStatement(tableSchema, conditions);
            return connection.ExecuteCommandAsync(sql, parameters, transaction, commandTimeout);
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
        public static Task<SqlCommandResult> DeleteAllAsync<TEntity>(
            this IDbConnection connection,
            IDbTransaction transaction = null,
            IDialect dialect = null,
            int? commandTimeout = null)
        {
            Ensure.NotNull(connection, nameof(connection));
            dialect = dialect ?? MicroCRUDConfig.DefaultDialect;

            var tableSchema = TableSchemaFactory.GetTableSchema(typeof(TEntity), dialect);
            var sql = dialect.MakeDeleteRangeStatement(tableSchema, null);
            return connection.ExecuteCommandAsync(sql, null, transaction, commandTimeout);
        }

        private static async Task<SqlCommandResult> ExecuteCommandAsync(
            this IDbConnection connection,
            string sql,
            object param,
            IDbTransaction transaction,
            int? commandTimeout)
        {
            var numRowsAffected = await connection.ExecuteAsync(sql, param, transaction, commandTimeout)
                                                  .ConfigureAwait(false);
            return new SqlCommandResult(numRowsAffected);
        }
    }
}