// ReSharper disable once CheckNamespace
namespace Dapper
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using Dapper.MicroCRUD.Dialects;
    using Dapper.MicroCRUD.SqlCommands;
    using Dapper.MicroCRUD.Utils;
    using Pagination;

    public static class DapperConnectionMicroCrudExtensions
    {
        /// <summary>
        /// Counts how many entities in the <typeparamref name="TEntity"/> table match the <paramref name="conditions"/>.
        /// </summary>
        /// <example>
        /// <code>
        /// <![CDATA[
        /// this.connection.Count<UserEntity>("WHERE Age > @MinAge", new { MinAge = 18 });
        /// ]]>
        /// </code>
        /// </example>
        public static int Count<TEntity>(this IDapperConnection connection, string conditions = null, object parameters = null, int? commandTimeout = null)
        {
            Ensure.NotNull(connection, nameof(connection));
            return connection.DbConnection.Count<TEntity>(conditions, parameters, connection.Transaction, connection.Dialect, commandTimeout);
        }

        /// <summary>
        /// Counts how many entities in the <typeparamref name="TEntity"/> table match the <paramref name="conditions"/>.
        /// </summary>
        /// <example>
        /// <code>
        /// <![CDATA[
        /// this.connection.Count<UserEntity>(new { Age = 18 });
        /// ]]>
        /// </code>
        /// </example>
        public static int Count<TEntity>(this IDapperConnection connection, object conditions, int? commandTimeout = null)
        {
            Ensure.NotNull(connection, nameof(connection));
            return connection.DbConnection.Count<TEntity>(conditions, connection.Transaction, connection.Dialect, commandTimeout);
        }

        /// <summary>
        /// Finds a single entity from the <typeparamref name="TEntity"/> table by it's primary key, or the default value if not found.
        /// </summary>
        /// <example>
        /// <code>
        /// <![CDATA[
        /// var entity = this.connection.Find<UserEntity>(12);
        /// ]]>
        /// </code>
        /// </example>
        public static TEntity Find<TEntity>(this IDapperConnection connection, object id, int? commandTimeout = null)
        {
            Ensure.NotNull(connection, nameof(connection));
            return connection.DbConnection.Find<TEntity>(id, connection.Transaction, connection.Dialect, commandTimeout);
        }

        /// <summary>
        /// Gets a single entity from the <typeparamref name="TEntity"/> table by it's primary key, or throws an exception if not found
        /// </summary>
        /// <example>
        /// <code>
        /// <![CDATA[
        /// var entity = this.connection.Get<UserEntity>(12);
        /// ]]>
        /// </code>
        /// </example>
        /// <exception cref="InvalidOperationException">The entity was not found.</exception>
        public static TEntity Get<TEntity>(
            this IDapperConnection connection,
            object id,
            int? commandTimeout = null)
            where TEntity : class
        {
            Ensure.NotNull(connection, nameof(connection));
            return connection.DbConnection.Get<TEntity>(id, connection.Transaction, connection.Dialect, commandTimeout);
        }

        /// <summary>
        /// Gets the first matching entity from the <typeparamref name="TEntity"/> table which matches the <paramref name="conditions"/>,
        /// or the default value if none match.
        /// </summary>
        /// <example>
        /// <code>
        /// <![CDATA[
        /// var user = this.connection.GetFirstOrDefault<UserEntity>("WHERE Age > @MinAge", new { MinAge = 18 });
        /// ]]>
        /// </code>
        /// </example>
        public static TEntity GetFirstOrDefault<TEntity>(
            this IDapperConnection connection,
            string conditions,
            string orderBy,
            object parameters = null,
            int? commandTimeout = null)
        {
            Ensure.NotNull(connection, nameof(connection));

            return connection.DbConnection
                             .GetFirstOrDefault<TEntity>(conditions, orderBy, parameters, connection.Transaction, connection.Dialect, commandTimeout);
        }

        /// <summary>
        /// Gets the first matching entity from the <typeparamref name="TEntity"/> table which matches the <paramref name="conditions"/>,
        /// or the default value if none match.
        /// </summary>
        /// <example>
        /// <code>
        /// <![CDATA[
        /// var user = this.connection.GetFirstOrDefault<UserEntity>(new { Age = 18 });
        /// ]]>
        /// </code>
        /// </example>
        public static TEntity GetFirstOrDefault<TEntity>(
            this IDapperConnection connection,
            object conditions,
            string orderBy,
            int? commandTimeout = null)
            where TEntity : class
        {
            Ensure.NotNull(connection, nameof(connection));

            return connection.DbConnection.GetFirstOrDefault<TEntity>(conditions, orderBy, connection.Transaction, connection.Dialect, commandTimeout);
        }

        /// <summary>
        /// Gets the first matching entity from the <typeparamref name="TEntity"/> table which matches the <paramref name="conditions"/>,
        /// or throws an <see cref="InvalidOperationException"/> if none match.
        /// </summary>
        /// <example>
        /// <code>
        /// <![CDATA[
        /// var user = this.connection.GetFirst<UserEntity>("WHERE Age > @MinAge", new { MinAge = 18 });
        /// ]]>
        /// </code>
        /// </example>
        public static TEntity GetFirst<TEntity>(
            this IDapperConnection connection,
            string conditions,
            string orderBy,
            object parameters = null,
            int? commandTimeout = null)
        {
            Ensure.NotNull(connection, nameof(connection));

            return connection.DbConnection.GetFirst<TEntity>(conditions, orderBy, parameters, connection.Transaction, connection.Dialect, commandTimeout);
        }

        /// <summary>
        /// Gets the first matching entity from the <typeparamref name="TEntity"/> table which matches the <paramref name="conditions"/>,
        /// or throws an <see cref="InvalidOperationException"/> if none match.
        /// </summary>
        /// <example>
        /// <code>
        /// <![CDATA[
        /// var user = this.connection.GetFirst<UserEntity>(new { Age = 18 });
        /// ]]>
        /// </code>
        /// </example>
        public static TEntity GetFirst<TEntity>(
            this IDapperConnection connection,
            object conditions,
            string orderBy,
            int? commandTimeout = null)
        {
            Ensure.NotNull(connection, nameof(connection));

            return connection.DbConnection.GetFirst<TEntity>(conditions, orderBy, connection.Transaction, connection.Dialect, commandTimeout);
        }

        /// <summary>
        /// Gets the only matching entity from the <typeparamref name="TEntity"/> table which matches the <paramref name="conditions"/>,
        /// or the default value if none match. Throws an <see cref="InvalidOperationException"/> if multiple entities match.
        /// </summary>
        /// <example>
        /// <code>
        /// <![CDATA[
        /// var user = this.connection.GetSingleOrDefault<UserEntity>("WHERE Age > @MinAge", new { MinAge = 18 });
        /// ]]>
        /// </code>
        /// </example>
        public static TEntity GetSingleOrDefault<TEntity>(
            this IDapperConnection connection,
            string conditions,
            object parameters = null,
            int? commandTimeout = null)
        {
            Ensure.NotNull(connection, nameof(connection));

            return connection.DbConnection.GetSingleOrDefault<TEntity>(conditions, parameters, connection.Transaction, connection.Dialect, commandTimeout);
        }

        /// <summary>
        /// Gets the only matching entity from the <typeparamref name="TEntity"/> table which matches the <paramref name="conditions"/>,
        /// or the default value if none match. Throws an <see cref="InvalidOperationException"/> if multiple entities match.
        /// </summary>
        /// <example>
        /// <code>
        /// <![CDATA[
        /// var user = this.connection.GetSingleOrDefault<UserEntity>(new { Age = 18 });
        /// ]]>
        /// </code>
        /// </example>
        public static TEntity GetSingleOrDefault<TEntity>(
            this IDapperConnection connection,
            object conditions,
            int? commandTimeout = null)
            where TEntity : class
        {
            Ensure.NotNull(connection, nameof(connection));

            return connection.DbConnection.GetSingleOrDefault<TEntity>(conditions, connection.Transaction, connection.Dialect, commandTimeout);
        }

        /// <summary>
        /// Gets the only matching entity from the <typeparamref name="TEntity"/> table which matches the <paramref name="conditions"/>,
        /// or throws an <see cref="InvalidOperationException"/> if no entries, or multiple entities match.
        /// </summary>
        /// <example>
        /// <code>
        /// <![CDATA[
        /// var user = this.connection.GetSingle<UserEntity>("WHERE Age > @MinAge", new { MinAge = 18 });
        /// ]]>
        /// </code>
        /// </example>
        public static TEntity GetSingle<TEntity>(
            this IDapperConnection connection,
            string conditions,
            object parameters = null,
            int? commandTimeout = null)
        {
            Ensure.NotNull(connection, nameof(connection));

            return connection.DbConnection.GetSingle<TEntity>(conditions, parameters, connection.Transaction, connection.Dialect, commandTimeout);
        }

        /// <summary>
        /// Gets the only matching entity from the <typeparamref name="TEntity"/> table which matches the <paramref name="conditions"/>,
        /// or throws an <see cref="InvalidOperationException"/> if no entries, or multiple entities match.
        /// </summary>
        /// <example>
        /// <code>
        /// <![CDATA[
        /// var user = this.connection.GetSingle<UserEntity>(new { Age = 18 });
        /// ]]>
        /// </code>
        /// </example>
        public static TEntity GetSingle<TEntity>(
            this IDapperConnection connection,
            object conditions,
            int? commandTimeout = null)
        {
            Ensure.NotNull(connection, nameof(connection));

            return connection.DbConnection.GetSingle<TEntity>(conditions, connection.Transaction, connection.Dialect, commandTimeout);
        }

        /// <summary>
        /// Gets a collection of entities from the <typeparamref name="TEntity"/> table which match the <paramref name="conditions"/>.
        /// </summary>
        /// <example>
        /// <code>
        /// <![CDATA[
        /// var users = this.connection.GetRange<UserEntity>("WHERE Age > @MinAge", new { MinAge = 18 });
        /// ]]>
        /// </code>
        /// </example>
        public static IEnumerable<TEntity> GetRange<TEntity>(
            this IDapperConnection connection,
            string conditions,
            object parameters = null,
            int? commandTimeout = null)
        {
            Ensure.NotNull(connection, nameof(connection));
            return connection.DbConnection.GetRange<TEntity>(conditions, parameters, connection.Transaction, connection.Dialect, commandTimeout);
        }

        /// <summary>
        /// Gets a collection of entities from the <typeparamref name="TEntity"/> table which match the <paramref name="conditions"/>.
        /// </summary>
        /// <example>
        /// <code>
        /// <![CDATA[
        /// var users = this.connection.GetRange<UserEntity>(new { Age = 18 });
        /// ]]>
        /// </code>
        /// </example>
        public static IEnumerable<TEntity> GetRange<TEntity>(this IDapperConnection connection, object conditions, int? commandTimeout = null)
        {
            Ensure.NotNull(connection, nameof(connection));
            return connection.DbConnection.GetRange<TEntity>(conditions, connection.Transaction, connection.Dialect, commandTimeout);
        }

        /// <summary>
        /// Gets a collection of entities from the <typeparamref name="TEntity"/> table which match the <paramref name="conditions"/>.
        /// </summary>
        /// <example>
        /// <code>
        /// <![CDATA[
        /// var pageBuilder = new PageIndexPageBuilder(3, 10);
        /// var users = this.connection.GetPage<UserEntity>(pageBuilder, "WHERE Age > @MinAge", "Age DESC", new { MinAge = 18 });
        /// ]]>
        /// </code>
        /// </example>
        public static PagedList<TEntity> GetPage<TEntity>(
            this IDapperConnection connection,
            IPageBuilder pageBuilder,
            string conditions,
            string orderBy,
            object parameters = null,
            int? commandTimeout = null)
        {
            Ensure.NotNull(connection, nameof(connection));

            return connection.DbConnection
                             .GetPage<TEntity>(pageBuilder, conditions, orderBy, parameters, connection.Transaction, connection.Dialect, commandTimeout);
        }

        /// <summary>
        /// Gets a collection of entities from the <typeparamref name="TEntity"/> table which match the <paramref name="conditions"/>.
        /// </summary>
        /// <example>
        /// <code>
        /// <![CDATA[
        /// var pageBuilder = new PageIndexPageBuilder(3, 10);
        /// var users = this.connection.GetPage<UserEntity>(pageBuilder, new { Age = 10 }, "Age DESC");
        /// ]]>
        /// </code>
        /// </example>
        public static PagedList<TEntity> GetPage<TEntity>(
            this IDapperConnection connection,
            IPageBuilder pageBuilder,
            object conditions,
            string orderBy,
            IDbTransaction transaction = null,
            IDialect dialect = null,
            int? commandTimeout = null)
        {
            Ensure.NotNull(connection, nameof(connection));

            return connection.DbConnection.GetPage<TEntity>(pageBuilder, conditions, orderBy, connection.Transaction, connection.Dialect, commandTimeout);
        }

        /// <summary>
        /// Gets all the entities in the <typeparamref name="TEntity"/> table.
        /// </summary>
        public static IEnumerable<TEntity> GetAll<TEntity>(this IDapperConnection connection, int? commandTimeout = null)
        {
            Ensure.NotNull(connection, nameof(connection));
            return connection.DbConnection.GetAll<TEntity>(connection.Transaction, connection.Dialect, commandTimeout);
        }

        /// <summary>
        /// Inserts the <paramref name="entity"/> into the database.
        /// </summary>
        /// <example>
        /// <code>
        /// <![CDATA[
        /// var entity = new User { Name = "Little bobby tables" };
        /// this.connection.Insert(entity);
        /// ]]>
        /// </code>
        /// </example>
        public static void Insert(this IDapperConnection connection, object entity, int? commandTimeout = null, bool? verifyAffectedRowCount = null)
        {
            Ensure.NotNull(connection, nameof(connection));
            connection.DbConnection.Insert(entity, connection.Transaction, connection.Dialect, commandTimeout, verifyAffectedRowCount);
        }

        /// <summary>
        /// Inserts the <paramref name="entity"/> into the database, and returns the auto-generated identity (or the default if invalid)
        /// </summary>
        /// <example>
        /// <code>
        /// <![CDATA[
        /// var entity = new User { Name = "Little bobby tables" };
        /// entity.Id = this.connection.Insert<int>(entity);
        /// ]]>
        /// </code>
        /// </example>
        public static TPrimaryKey Insert<TPrimaryKey>(this IDapperConnection connection, object entity, int? commandTimeout = null)
        {
            Ensure.NotNull(connection, nameof(connection));
            return connection.DbConnection.Insert<TPrimaryKey>(entity, connection.Transaction, connection.Dialect, commandTimeout);
        }

        /// <summary>
        /// <para>Efficiently inserts multiple <paramref name="entities"/> into the database.</para>
        /// <para>For performance, it's recommended to always perform this action inside of a transaction.</para>
        /// </summary>
        /// <example>
        /// <code>
        /// <![CDATA[
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
        public static SqlCommandResult InsertRange<TEntity>(this IDapperConnection connection, IEnumerable<TEntity> entities, int? commandTimeout = null)
        {
            Ensure.NotNull(connection, nameof(connection));
            return connection.DbConnection.InsertRange(entities, connection.Transaction, connection.Dialect, commandTimeout);
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
            this IDapperConnection connection,
            IEnumerable<TEntity> entities,
            Action<TEntity, TPrimaryKey> setPrimaryKey,
            int? commandTimeout = null)
        {
            Ensure.NotNull(connection, nameof(connection));
            connection.DbConnection.InsertRange(entities, setPrimaryKey, connection.Transaction, connection.Dialect, commandTimeout);
        }

        /// <summary>
        /// Updates the <paramref name="entity"/> in the database.
        /// </summary>
        /// <example>
        /// <code>
        /// <![CDATA[
        /// var entity = this.connection.Find<UserEntity>(5);
        /// entity.Name = "Little bobby tables";
        /// this.connection.Update(entity);
        /// ]]>
        /// </code>
        /// </example>
        /// <exception cref="AffectedRowCountException">The update command didn't change any record, or changed multiple records.</exception>
        public static void Update<TEntity>(
            this IDapperConnection connection,
            TEntity entity,
            int? commandTimeout = null,
            bool? verifyAffectedRowCount = null)
        {
            Ensure.NotNull(connection, nameof(connection));
            connection.DbConnection.Update(entity, connection.Transaction, connection.Dialect, commandTimeout, verifyAffectedRowCount);
        }

        /// <summary>
        /// Efficiently updates multiple <paramref name="entities"/> in the database.
        /// </summary>
        /// <example>
        /// <code>
        /// <![CDATA[
        /// using (var transaction = this.connection.BeginTransaction())
        /// {
        ///     var entities = this.connection.GetRange<UserEntity>("WHERE @Age = 10", transaction);
        ///
        ///     foreach (var entity in entities)
        ///     {
        ///         entity.Name = "Little bobby tables";
        ///     }
        ///
        ///     this.connection.UpdateRange(entities, transaction);
        ///     transaction.Commit();
        /// }
        /// ]]>
        /// </code>
        /// </example>
        /// <returns>The number of affected records.</returns>
        public static SqlCommandResult UpdateRange<TEntity>(this IDapperConnection connection, IEnumerable<TEntity> entities, int? commandTimeout = null)
        {
            Ensure.NotNull(connection, nameof(connection));
            return connection.DbConnection.UpdateRange(entities, connection.Transaction, connection.Dialect, commandTimeout);
        }

        /// <summary>
        /// Deletes the entity in the <typeparamref name="TEntity"/> table, identified by its primary key.
        /// </summary>
        /// <example>
        /// <code>
        /// <![CDATA[
        /// var entity = this.connection.Find<UserEntity>(5);
        /// this.connection.Delete(entity);
        /// ]]>
        /// </code>
        /// </example>
        /// <exception cref="AffectedRowCountException">The delete command didn't delete anything, or deleted multiple records.</exception>
        public static void Delete<TEntity>(
            this IDapperConnection connection,
            TEntity entity,
            int? commandTimeout = null,
            bool? verifyAffectedRowCount = null)
        {
            Ensure.NotNull(connection, nameof(connection));
            connection.DbConnection.Delete(entity, connection.Transaction, connection.Dialect, commandTimeout, verifyAffectedRowCount);
        }

        /// <summary>
        /// Deletes the entity in the <typeparamref name="TEntity"/> table which has the <paramref name="id"/>.
        /// </summary>
        /// <example>
        /// <code>
        /// <![CDATA[
        /// this.connection.Delete<UserEntity>(5);
        /// ]]>
        /// </code>
        /// </example>
        /// <exception cref="AffectedRowCountException">The delete command didn't delete anything, or deleted multiple records.</exception>
        public static void Delete<TEntity>(
            this IDapperConnection connection,
            object id,
            int? commandTimeout = null,
            bool? verifyAffectedRowCount = null)
        {
            Ensure.NotNull(connection, nameof(connection));
            connection.DbConnection.Delete<TEntity>(id, connection.Transaction, connection.Dialect, commandTimeout, verifyAffectedRowCount);
        }

        /// <summary>
        /// <para>Deletes all the entities in the <typeparamref name="TEntity"/> table which match the <paramref name="conditions"/>.</para>
        /// <para>Note: <paramref name="conditions"/> must contain a WHERE clause. Use <see cref="DeleteAll{TEntity}"/> if you want to delete all entities.</para>
        /// </summary>
        /// <example>
        /// <code>
        /// <![CDATA[
        /// this.connection.DeleteRange<UserEntity>("WHERE Name LIKE '%Foo%'");
        /// ]]>
        /// </code>
        /// </example>
        /// <returns>The number of deleted entities.</returns>
        public static SqlCommandResult DeleteRange<TEntity>(
            this IDapperConnection connection,
            string conditions,
            object parameters = null,
            int? commandTimeout = null)
        {
            Ensure.NotNull(connection, nameof(connection));
            return connection.DbConnection.DeleteRange<TEntity>(conditions, parameters, connection.Transaction, connection.Dialect, commandTimeout);
        }

        /// <summary>
        /// <para>Deletes all the entities in the <typeparamref name="TEntity"/> table which match the <paramref name="conditions"/>.</para>
        /// <para>Note: <paramref name="conditions"/> must contain a WHERE clause. Use <see cref="DeleteAll{TEntity}"/> if you want to delete all entities.</para>
        /// </summary>
        /// <example>
        /// <code>
        /// <![CDATA[
        /// this.connection.DeleteRange<UserEntity>("WHERE Name LIKE '%Foo%'");
        /// ]]>
        /// </code>
        /// </example>
        /// <returns>The number of deleted entities.</returns>
        public static SqlCommandResult DeleteRange<TEntity>(this IDapperConnection connection, object conditions, int? commandTimeout = null)
        {
            Ensure.NotNull(connection, nameof(connection));
            return connection.DbConnection.DeleteRange<TEntity>(conditions, connection.Transaction, connection.Dialect, commandTimeout);
        }

        /// <summary>
        /// Deletes all the entities in the <typeparamref name="TEntity"/> table.
        /// </summary>
        /// <example>
        /// <code>
        /// <![CDATA[
        /// this.connection.DeleteAll<UserEntity>();
        /// ]]>
        /// </code>
        /// </example>
        /// <returns>The number of deleted entities.</returns>
        public static SqlCommandResult DeleteAll<TEntity>(this IDapperConnection connection, int? commandTimeout = null)
        {
            Ensure.NotNull(connection, nameof(connection));
            return connection.DbConnection.DeleteAll<TEntity>(connection.Transaction, connection.Dialect, commandTimeout);
        }
    }
}