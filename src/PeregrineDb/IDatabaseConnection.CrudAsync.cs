namespace PeregrineDb
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using Pagination;
    using PeregrineDb.SqlCommands;

    public partial interface IDatabaseConnection
    {
        /// <summary>
        /// Counts how many entities in the <typeparamref name="TEntity"/> table match the <paramref name="conditions"/>.
        /// </summary>
        /// <example>
        /// <code>
        /// <![CDATA[
        /// await databaseConnection.CountAsync<UserEntity>("WHERE Age > @MinAge", new { MinAge = 18 });
        /// ]]>
        /// </code>
        /// </example>
        Task<int> CountAsync<TEntity>(
            string conditions = null,
            object parameters = null,
            int? commandTimeout = null,
            CancellationToken cancellationToken = default);

        /// <summary>
        /// Counts how many entities in the <typeparamref name="TEntity"/> table match the <paramref name="conditions"/>.
        /// </summary>
        /// <example>
        /// <code>
        /// <![CDATA[
        /// await databaseConnection.CountAsync<UserEntity>(new { Age = 18 });
        /// ]]>
        /// </code>
        /// </example>
        Task<int> CountAsync<TEntity>(object conditions, int? commandTimeout = null, CancellationToken cancellationToken = default);

        /// <summary>
        /// Finds a single entity from the <typeparamref name="TEntity"/> table by it's primary key, or the default value if not found.
        /// </summary>
        /// <example>
        /// <code>
        /// <![CDATA[
        /// var entity = await databaseConnection.FindAsync<UserEntity>(12);
        /// ]]>
        /// </code>
        /// </example>
        Task<TEntity> FindAsync<TEntity>(object id, int? commandTimeout = null, CancellationToken cancellationToken = default);

        /// <summary>
        /// Gets a single entity from the <typeparamref name="TEntity"/> table by it's primary key, or throws an exception if not found
        /// </summary>
        /// <example>
        /// <code>
        /// <![CDATA[
        /// var entity = await databaseConnection.GetAsync<UserEntity>(12);
        /// ]]>
        /// </code>
        /// </example>
        /// <exception cref="InvalidOperationException">The entity was not found.</exception>
        Task<TEntity> GetAsync<TEntity>(object id, int? commandTimeout = null, CancellationToken cancellationToken = default)
            where TEntity : class;

        /// <summary>
        /// Gets the first matching entity from the <typeparamref name="TEntity"/> table which matches the <paramref name="conditions"/>,
        /// or the default value if none match.
        /// </summary>
        /// <example>
        /// <code>
        /// <![CDATA[
        /// var user = await databaseConnection.GetFirstOrDefaultAsync<UserEntity>("WHERE Age > @MinAge", new { MinAge = 18 });
        /// ]]>
        /// </code>
        /// </example>
        Task<TEntity> GetFirstOrDefaultAsync<TEntity>(
            string conditions,
            string orderBy,
            object parameters = null,
            int? commandTimeout = null,
            CancellationToken cancellationToken = default);

        /// <summary>
        /// Gets the first matching entity from the <typeparamref name="TEntity"/> table which matches the <paramref name="conditions"/>,
        /// or the default value if none match.
        /// </summary>
        /// <example>
        /// <code>
        /// <![CDATA[
        /// var user = await databaseConnection.GetFirstOrDefaultAsync<UserEntity>(new { Age = 18 });
        /// ]]>
        /// </code>
        /// </example>
        Task<TEntity> GetFirstOrDefaultAsync<TEntity>(
            object conditions,
            string orderBy,
            int? commandTimeout = null,
            CancellationToken cancellationToken = default);

        /// <summary>
        /// Gets the first matching entity from the <typeparamref name="TEntity"/> table which matches the <paramref name="conditions"/>,
        /// or throws an <see cref="InvalidOperationException"/> if none match.
        /// </summary>
        /// <example>
        /// <code>
        /// <![CDATA[
        /// var user = await databaseConnection.GetFirstAsync<UserEntity>("WHERE Age > @MinAge", new { MinAge = 18 });
        /// ]]>
        /// </code>
        /// </example>
        Task<TEntity> GetFirstAsync<TEntity>(
            string conditions,
            string orderBy,
            object parameters = null,
            int? commandTimeout = null,
            CancellationToken cancellationToken = default)
            where TEntity : class;

        /// <summary>
        /// Gets the first matching entity from the <typeparamref name="TEntity"/> table which matches the <paramref name="conditions"/>,
        /// or throws an <see cref="InvalidOperationException"/> if none match.
        /// </summary>
        /// <example>
        /// <code>
        /// <![CDATA[
        /// var user = await databaseConnection.GetFirstAsync<UserEntity>(new { Age = 18 });
        /// ]]>
        /// </code>
        /// </example>
        Task<TEntity> GetFirstAsync<TEntity>(object conditions, string orderBy, int? commandTimeout = null, CancellationToken cancellationToken = default)
            where TEntity : class;

        /// <summary>
        /// Gets the only matching entity from the <typeparamref name="TEntity"/> table which matches the <paramref name="conditions"/>,
        /// or the default value if none match. Throws an <see cref="InvalidOperationException"/> if multiple entities match.
        /// </summary>
        /// <example>
        /// <code>
        /// <![CDATA[
        /// var user = await databaseConnection.GetSingleOrDefaultAsync<UserEntity>("WHERE Age > @MinAge", new { MinAge = 18 });
        /// ]]>
        /// </code>
        /// </example>
        Task<TEntity> GetSingleOrDefaultAsync<TEntity>(
            string conditions,
            object parameters = null,
            int? commandTimeout = null,
            CancellationToken cancellationToken = default);

        /// <summary>
        /// Gets the only matching entity from the <typeparamref name="TEntity"/> table which matches the <paramref name="conditions"/>,
        /// or the default value if none match. Throws an <see cref="InvalidOperationException"/> if multiple entities match.
        /// </summary>
        /// <example>
        /// <code>
        /// <![CDATA[
        /// var user = await databaseConnection.GetSingleOrDefaultAsync<UserEntity>(new { Age = 18 });
        /// ]]>
        /// </code>
        /// </example>
        Task<TEntity> GetSingleOrDefaultAsync<TEntity>(object conditions, int? commandTimeout = null, CancellationToken cancellationToken = default);

        /// <summary>
        /// Gets the only matching entity from the <typeparamref name="TEntity"/> table which matches the <paramref name="conditions"/>,
        /// or throws an <see cref="InvalidOperationException"/> if no entries, or multiple entities match.
        /// </summary>
        /// <example>
        /// <code>
        /// <![CDATA[
        /// var user = await databaseConnection.GetSingleAsync<UserEntity>("WHERE Age > @MinAge", new { MinAge = 18 });
        /// ]]>
        /// </code>
        /// </example>
        Task<TEntity> GetSingleAsync<TEntity>(
            string conditions,
            object parameters = null,
            int? commandTimeout = null,
            CancellationToken cancellationToken = default)
            where TEntity : class;

        /// <summary>
        /// Gets the only matching entity from the <typeparamref name="TEntity"/> table which matches the <paramref name="conditions"/>,
        /// or throws an <see cref="InvalidOperationException"/> if no entries, or multiple entities match.
        /// </summary>
        /// <example>
        /// <code>
        /// <![CDATA[
        /// var user = await databaseConnection.GetSingleAsync<UserEntity>(new { Age = 18 });
        /// ]]>
        /// </code>
        /// </example>
        Task<TEntity> GetSingleAsync<TEntity>(object conditions, int? commandTimeout = null, CancellationToken cancellationToken = default)
            where TEntity : class;

        /// <summary>
        /// Gets a collection of entities from the <typeparamref name="TEntity"/> table which match the <paramref name="conditions"/>.
        /// </summary>
        /// <example>
        /// <code>
        /// <![CDATA[
        /// var users = await databaseConnection.GetRangeAsync<UserEntity>("WHERE Age > @MinAge", new { MinAge = 18 });
        /// ]]>
        /// </code>
        /// </example>
        Task<IEnumerable<TEntity>> GetRangeAsync<TEntity>(
            string conditions,
            object parameters = null,
            int? commandTimeout = null,
            CancellationToken cancellationToken = default);

        /// <summary>
        /// Gets a collection of entities from the <typeparamref name="TEntity"/> table which match the <paramref name="conditions"/>.
        /// </summary>
        /// <example>
        /// <code>
        /// <![CDATA[
        /// var users = await databaseConnection.GetRangeAsync<UserEntity>(new { Age = 18 });
        /// ]]>
        /// </code>
        /// </example>
        Task<IEnumerable<TEntity>> GetRangeAsync<TEntity>(object conditions, int? commandTimeout = null, CancellationToken cancellationToken = default);

        /// <summary>
        /// Gets a collection of entities from the <typeparamref name="TEntity"/> table which match the <paramref name="conditions"/>.
        /// </summary>
        /// <example>
        /// <code>
        /// <![CDATA[
        /// var pageBuilder = new PageIndexPageBuilder(3, 10);
        /// var users = await databaseConnection.GetPageAsync<UserEntity>(pageBuilder, "WHERE Age > @MinAge", "Age DESC", new { MinAge = 18 });
        /// ]]>
        /// </code>
        /// </example>
        Task<PagedList<TEntity>> GetPageAsync<TEntity>(
            IPageBuilder pageBuilder,
            string conditions,
            string orderBy,
            object parameters = null,
            int? commandTimeout = null,
            CancellationToken cancellationToken = default);

        /// <summary>
        /// Gets a collection of entities from the <typeparamref name="TEntity"/> table which match the <paramref name="conditions"/>.
        /// </summary>
        /// <example>
        /// <code>
        /// <![CDATA[
        /// ...
        /// var pageBuilder = new PageIndexPageBuilder(3, 10);
        /// var users = await databaseConnection.GetPageAsync<UserEntity>(pageBuilder, new { Age = 10 }, "Age DESC");
        /// ]]>
        /// </code>
        /// </example>
        Task<PagedList<TEntity>> GetPageAsync<TEntity>(
            IPageBuilder pageBuilder,
            object conditions,
            string orderBy,
            int? commandTimeout = null,
            CancellationToken cancellationToken = default);

        /// <summary>
        /// Gets all the entities in the <typeparamref name="TEntity"/> table.
        /// </summary>
        Task<IEnumerable<TEntity>> GetAllAsync<TEntity>(int? commandTimeout = null);

        /// <summary>
        /// Inserts the <paramref name="entity"/> into the databaseConnection.
        /// </summary>
        /// <example>
        /// <code>
        /// <![CDATA[
        /// var entity = new User { Name = "Little bobby tables" };
        /// await databaseConnection.InsertAsync(entity);
        /// ]]>
        /// </code>
        /// </example>
        Task InsertAsync(object entity, int? commandTimeout = null, bool? verifyAffectedRowCount = null, CancellationToken cancellationToken = default);

        /// <summary>
        /// Inserts the <paramref name="entity"/> into the databaseConnection, and returns the auto-generated identity (or the default if invalid)
        /// </summary>
        /// <example>
        /// <code>
        /// <![CDATA[
        /// var entity = new User { Name = "Little bobby tables" };
        /// entity.Id = await databaseConnection.InsertAsync<int>(entity);
        /// ]]>
        /// </code>
        /// </example>
        Task<TPrimaryKey> InsertAsync<TPrimaryKey>(object entity, int? commandTimeout = null, CancellationToken cancellationToken = default);

        /// <summary>
        /// <para>Efficiently inserts multiple <paramref name="entities"/> into the databaseConnection.</para>
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
        /// using (var databaseConnection = databaseProvider.StartUnitOfWork())
        /// {
        ///     await databaseConnection.InsertRangeAsync(entities);
        ///
        ///     databaseConnection.SaveChanges();
        /// }
        /// ]]>
        /// </code>
        /// </example>
        Task<SqlCommandResult> InsertRangeAsync<TEntity>(
            IEnumerable<TEntity> entities,
            int? commandTimeout = null,
            CancellationToken cancellationToken = default);

        /// <summary>
        /// <para>
        /// Efficiently inserts multiple <paramref name="entities"/> into the databaseConnection,
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
        /// using (var databaseConnection = databaseProvider.StartUnitOfWork())
        /// {
        ///     await databaseConnection.InsertRangeAsync<User, int>(entities, (e, k) => { e.Id = k; });
        ///
        ///     databaseConnection.SaveChanges();
        /// }
        /// ]]>
        /// </code>
        /// </example>
        Task InsertRangeAsync<TEntity, TPrimaryKey>(
            IEnumerable<TEntity> entities,
            Action<TEntity, TPrimaryKey> setPrimaryKey,
            int? commandTimeout = null,
            CancellationToken cancellationToken = default);

        /// <summary>
        /// Updates the <paramref name="entity"/> in the databaseConnection.
        /// </summary>
        /// <example>
        /// <code>
        /// <![CDATA[
        /// var entity = databaseConnection.Get<UserEntity>(5);
        /// entity.Name = "Little bobby tables";
        /// await databaseConnection.UpdateAsync(entity);
        /// ]]>
        /// </code>
        /// </example>
        /// <exception cref="AffectedRowCountException">The update command didn't change any record, or changed multiple records.</exception>
        Task UpdateAsync<TEntity>(
            TEntity entity,
            int? commandTimeout = null,
            bool? verifyAffectedRowCount = null,
            CancellationToken cancellationToken = default);

        /// <summary>
        /// <para>Efficiently updates multiple <paramref name="entities"/> in the databaseConnection.</para>
        /// <para>For performance, it's recommended to always perform this action inside of a transaction.</para>
        /// </summary>
        /// <example>
        /// <code>
        /// <![CDATA[
        /// using (var databaseConnection = databaseProvider.StartUnitOfWork())
        /// {
        ///     var entities = databaseConnection.GetRange<UserEntity>("WHERE @Age = 10");
        ///
        ///     foreach (var entity in entities)
        ///     {
        ///         entity.Name = "Little bobby tables";
        ///     }
        ///
        ///     await databaseConnection.UpdateRangeAsync(entities);
        ///     databaseConnection.SaveChanges();
        /// }
        /// ]]>
        /// </code>
        /// </example>
        /// <returns>The number of affected records.</returns>
        Task<SqlCommandResult> UpdateRangeAsync<TEntity>(
            IEnumerable<TEntity> entities,
            int? commandTimeout = null,
            CancellationToken cancellationToken = default);

        /// <summary>
        /// Deletes the entity in the <typeparamref name="TEntity"/> table, identified by its primary key.
        /// </summary>
        /// <example>
        /// <code>
        /// <![CDATA[
        /// var entity = databaseConnection.Get<UserEntity>(5);
        /// await databaseConnection.DeleteAsync(entity);
        /// ]]>
        /// </code>
        /// </example>
        /// <exception cref="AffectedRowCountException">The delete command didn't delete anything, or deleted multiple records.</exception>
        Task DeleteAsync<TEntity>(
            TEntity entity,
            int? commandTimeout = null,
            bool? verifyAffectedRowCount = null,
            CancellationToken cancellationToken = default);

        /// <summary>
        /// Deletes the entity in the <typeparamref name="TEntity"/> table which has the <paramref name="id"/>.
        /// </summary>
        /// <example>
        /// <code>
        /// await databaseConnection.DeleteAsync(5);
        /// </code>
        /// </example>
        /// <exception cref="AffectedRowCountException">The delete command didn't delete anything, or deleted multiple records.</exception>
        Task DeleteAsync<TEntity>(object id, int? commandTimeout = null, bool? verifyAffectedRowCount = null, CancellationToken cancellationToken = default);

        /// <summary>
        /// <para>Deletes all the entities in the <typeparamref name="TEntity"/> table which match the <paramref name="conditions"/>.</para>
        /// <para>Note: <paramref name="conditions"/> must contain a WHERE clause. Use <see cref="DeleteAll{TEntity}"/> if you want to delete all entities.</para>
        /// </summary>
        /// <example>
        /// <code>
        /// <![CDATA[
        /// await databaseConnection.DeleteRangeAsync<UserEntity>("WHERE Name LIKE '%Foo%'");
        /// ]]>
        /// </code>
        /// </example>
        /// <returns>The number of deleted entities.</returns>
        Task<SqlCommandResult> DeleteRangeAsync<TEntity>(
            string conditions,
            object parameters = null,
            int? commandTimeout = null,
            CancellationToken cancellationToken = default);

        /// <summary>
        /// <para>Deletes all the entities in the <typeparamref name="TEntity"/> table which match the <paramref name="conditions"/>.</para>
        /// <para>Note: <paramref name="conditions"/> must contain a WHERE clause. Use <see cref="DeleteAll{TEntity}"/> if you want to delete all entities.</para>
        /// </summary>
        /// <example>
        /// <code>
        /// <![CDATA[
        /// await databaseConnection.DeleteRangeAsync<UserEntity>("WHERE Name LIKE '%Foo%'");
        /// ]]>
        /// </code>
        /// </example>
        /// <returns>The number of deleted entities.</returns>
        Task<SqlCommandResult> DeleteRangeAsync<TEntity>(object conditions, int? commandTimeout = null, CancellationToken cancellationToken = default);

        /// <summary>
        /// Deletes all the entities in the <typeparamref name="TEntity"/> table.
        /// </summary>
        /// <example>
        /// <code>
        /// <![CDATA[
        /// await databaseConnection.DeleteAllAsync<UserEntity>();
        /// ]]>
        /// </code>
        /// </example>
        /// <returns>The number of deleted entities.</returns>
        Task<SqlCommandResult> DeleteAllAsync<TEntity>(int? commandTimeout = null, CancellationToken cancellationToken = default);
    }
}