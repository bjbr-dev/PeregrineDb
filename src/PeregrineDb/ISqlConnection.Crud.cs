namespace PeregrineDb
{
    using System;
    using System.Collections.Generic;
    using Pagination;
    using PeregrineDb.SqlCommands;

    public partial interface ISqlConnection
    {
        /// <summary>
        /// Counts how many entities in the <typeparamref name="TEntity"/> table match the <paramref name="conditions"/>.
        /// </summary>
        /// <example>
        /// <code>
        /// <![CDATA[
        /// var minAge = 18;
        /// databaseConnection.Count<DogEntity>($"WHERE Age > {minAge}");
        /// ]]>
        /// </code>
        /// </example>
        int Count<TEntity>(FormattableString conditions = null, int? commandTimeout = null);

        /// <summary>
        /// Counts how many entities in the <typeparamref name="TEntity"/> table match the <paramref name="conditions"/>.
        /// </summary>
        /// <example>
        /// <code>
        /// <![CDATA[
        /// databaseConnection.Count<DogEntity>(new { Age = 18 });
        /// ]]>
        /// </code>
        /// </example>
        int Count<TEntity>(object conditions, int? commandTimeout = null);

        /// <summary>
        /// Finds a single entity from the <typeparamref name="TEntity"/> table by it's primary key, or the default value if not found.
        /// </summary>
        /// <example>
        /// <code>
        /// <![CDATA[
        /// var entity = databaseConnection.Find<DogEntity>(12);
        /// ]]>
        /// </code>
        /// </example>
        TEntity Find<TEntity>(object id, int? commandTimeout = null);

        /// <summary>
        /// Gets a single entity from the <typeparamref name="TEntity"/> table by it's primary key, or throws an exception if not found
        /// </summary>
        /// <example>
        /// <code>
        /// <![CDATA[
        /// var entity = databaseConnection.Get<DogEntity>(12);
        /// ]]>
        /// </code>
        /// </example>
        /// <exception cref="InvalidOperationException">The entity was not found.</exception>
        TEntity Get<TEntity>(object id, int? commandTimeout = null)
            where TEntity : class;

        /// <summary>
        /// Gets the first matching entity from the <typeparamref name="TEntity"/> table which matches the <paramref name="conditions"/>,
        /// or the default value if none match.
        /// </summary>
        /// <example>
        /// <code>
        /// <![CDATA[
        /// var minAge = 18;
        /// var dog = databaseConnection.GetFirstOrDefault<DogEntity>($"WHERE Age > {minAge}");
        /// ]]>
        /// </code>
        /// </example>
        TEntity GetFirstOrDefault<TEntity>(FormattableString conditions, string orderBy, int? commandTimeout = null);

        /// <summary>
        /// Gets the first matching entity from the <typeparamref name="TEntity"/> table which matches the <paramref name="conditions"/>,
        /// or the default value if none match.
        /// </summary>
        /// <example>
        /// <code>
        /// <![CDATA[
        /// var dog = databaseConnection.GetFirstOrDefault<DogEntity>(new { Age = 18 });
        /// ]]>
        /// </code>
        /// </example>
        TEntity GetFirstOrDefault<TEntity>(object conditions, string orderBy, int? commandTimeout = null);

        /// <summary>
        /// Gets the first matching entity from the <typeparamref name="TEntity"/> table which matches the <paramref name="conditions"/>,
        /// or throws an <see cref="InvalidOperationException"/> if none match.
        /// </summary>
        /// <example>
        /// <code>
        /// <![CDATA[
        /// var minAge = 18;
        /// var dog = databaseConnection.GetFirst<DogEntity>($"WHERE Age > {minAge}");
        /// ]]>
        /// </code>
        /// </example>
        TEntity GetFirst<TEntity>(FormattableString conditions, string orderBy, int? commandTimeout = null)
            where TEntity : class;

        /// <summary>
        /// Gets the first matching entity from the <typeparamref name="TEntity"/> table which matches the <paramref name="conditions"/>,
        /// or throws an <see cref="InvalidOperationException"/> if none match.
        /// </summary>
        /// <example>
        /// <code>
        /// <![CDATA[
        /// var dog = databaseConnection.GetFirst<DogEntity>(new { Age = 18 });
        /// ]]>
        /// </code>
        /// </example>
        TEntity GetFirst<TEntity>(object conditions, string orderBy, int? commandTimeout = null)
            where TEntity : class;

        /// <summary>
        /// Gets the only matching entity from the <typeparamref name="TEntity"/> table which matches the <paramref name="conditions"/>,
        /// or the default value if none match. Throws an <see cref="InvalidOperationException"/> if multiple entities match.
        /// </summary>
        /// <example>
        /// <code>
        /// <![CDATA[
        /// var minAge = 18;
        /// var dog = databaseConnection.GetSingleOrDefault<DogEntity>($"WHERE Age > {minAge}");
        /// ]]>
        /// </code>
        /// </example>
        TEntity GetSingleOrDefault<TEntity>(FormattableString conditions, int? commandTimeout = null);

        /// <summary>
        /// Gets the only matching entity from the <typeparamref name="TEntity"/> table which matches the <paramref name="conditions"/>,
        /// or the default value if none match. Throws an <see cref="InvalidOperationException"/> if multiple entities match.
        /// </summary>
        /// <example>
        /// <code>
        /// <![CDATA[
        /// var dog = databaseConnection.GetSingleOrDefault<DogEntity>(new { Age = 18 });
        /// ]]>
        /// </code>
        /// </example>
        TEntity GetSingleOrDefault<TEntity>(object conditions, int? commandTimeout = null);

        /// <summary>
        /// Gets the only matching entity from the <typeparamref name="TEntity"/> table which matches the <paramref name="conditions"/>,
        /// or throws an <see cref="InvalidOperationException"/> if no entries, or multiple entities match.
        /// </summary>
        /// <example>
        /// <code>
        /// <![CDATA[
        /// var minAge = 18;
        /// var dog = databaseConnection.GetSingle<DogEntity>($"WHERE Age > {minAge}");
        /// ]]>
        /// </code>
        /// </example>
        TEntity GetSingle<TEntity>(FormattableString conditions, int? commandTimeout = null)
            where TEntity : class;

        /// <summary>
        /// Gets the only matching entity from the <typeparamref name="TEntity"/> table which matches the <paramref name="conditions"/>,
        /// or throws an <see cref="InvalidOperationException"/> if no entries, or multiple entities match.
        /// </summary>
        /// <example>
        /// <code>
        /// <![CDATA[
        /// var dog = databaseConnection.GetSingle<DogEntity>(new { Age = 18 });
        /// ]]>
        /// </code>
        /// </example>
        TEntity GetSingle<TEntity>(object conditions, int? commandTimeout = null)
            where TEntity : class;

        /// <summary>
        /// Gets a collection of entities from the <typeparamref name="TEntity"/> table which match the <paramref name="conditions"/>.
        /// </summary>
        /// <example>
        /// <code>
        /// <![CDATA[
        /// var minAge = 18;
        /// var dogs = databaseConnection.GetRange<DogEntity>($"WHERE Age > {minAge}");
        /// ]]>
        /// </code>
        /// </example>
        IEnumerable<TEntity> GetRange<TEntity>(FormattableString conditions, int? commandTimeout = null);

        /// <summary>
        /// Gets a collection of entities from the <typeparamref name="TEntity"/> table which match the <paramref name="conditions"/>.
        /// </summary>
        /// <example>
        /// <code>
        /// <![CDATA[
        /// var dogs = databaseConnection.GetRange<DogEntity>(new { Age = 18 });
        /// ]]>
        /// </code>
        /// </example>
        IEnumerable<TEntity> GetRange<TEntity>(object conditions, int? commandTimeout = null);

        /// <summary>
        /// Gets a collection of entities from the <typeparamref name="TEntity"/> table which match the <paramref name="conditions"/>.
        /// </summary>
        /// <example>
        /// <code>
        /// <![CDATA[
        /// var minAge = 18;
        /// var pageBuilder = new PageIndexPageBuilder(3, 10);
        /// var dogs = databaseConnection.GetPage<DogEntity>(pageBuilder, $"WHERE Age > {minAge}", "Age DESC");
        /// ]]>
        /// </code>
        /// </example>
        PagedList<TEntity> GetPage<TEntity>(IPageBuilder pageBuilder, FormattableString conditions, string orderBy, int? commandTimeout = null);

        /// <summary>
        /// Gets a collection of entities from the <typeparamref name="TEntity"/> table which match the <paramref name="conditions"/>.
        /// </summary>
        /// <example>
        /// <code>
        /// <![CDATA[
        /// ...
        /// var pageBuilder = new PageIndexPageBuilder(3, 10);
        /// var dogs = databaseConnection.GetPage<DogEntity>(pageBuilder, new { Age = 10 }, "Age DESC");
        /// ]]>
        /// </code>
        /// </example>
        PagedList<TEntity> GetPage<TEntity>(IPageBuilder pageBuilder, object conditions, string orderBy, int? commandTimeout = null);

        /// <summary>
        /// Gets all the entities in the <typeparamref name="TEntity"/> table.
        /// </summary>
        IEnumerable<TEntity> GetAll<TEntity>(int? commandTimeout = null);

        /// <summary>
        /// Inserts the <paramref name="entity"/> into the databaseConnection.
        /// </summary>
        /// <example>
        /// <code>
        /// <![CDATA[
        /// var entity = new dog { Name = "Little bobby tables" };
        /// databaseConnection.Insert(entity);
        /// ]]>
        /// </code>
        /// </example>
        void Insert(object entity, int? commandTimeout = null, bool? verifyAffectedRowCount = null);

        /// <summary>
        /// Inserts the <paramref name="entity"/> into the databaseConnection, and returns the auto-generated identity (or the default if invalid)
        /// </summary>
        /// <example>
        /// <code>
        /// <![CDATA[
        /// var entity = new dog { Name = "Little bobby tables" };
        /// entity.Id = databaseConnection.Insert<int>(entity);
        /// ]]>
        /// </code>
        /// </example>
        TPrimaryKey Insert<TPrimaryKey>(object entity, int? commandTimeout = null);

        /// <summary>
        /// <para>Efficiently inserts multiple <paramref name="entities"/> into the databaseConnection.</para>
        /// <para>For performance, it's recommended to always perform this action inside of a transaction.</para>
        /// </summary>
        /// <example>
        /// <code>
        /// <![CDATA[
        /// var entities = new []
        ///     {
        ///         new dog { Name = "Little bobby tables" },
        ///         new dog { Name = "Jimmy" };
        ///     };
        ///
        /// using (var databaseConnection = databaseProvider.StartUnitOfWork())
        /// {
        ///     databaseConnection.InsertRange(entities);
        ///
        ///     databaseConnection.SaveChanges();
        /// }
        /// ]]>
        /// </code>
        /// </example>
        CommandResult InsertRange<TEntity>(IEnumerable<TEntity> entities, int? commandTimeout = null);

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
        ///         new dog { Name = "Little bobby tables" },
        ///         new dog { Name = "Jimmy" };
        ///     };
        ///
        /// using (var databaseConnection = databaseProvider.StartUnitOfWork())
        /// {
        ///     databaseConnection.InsertRange<dog, int>(entities, (e, k) => { e.Id = k; });
        ///
        ///     databaseConnection.SaveChanges();
        /// }
        /// ]]>
        /// </code>
        /// </example>
        void InsertRange<TEntity, TPrimaryKey>(IEnumerable<TEntity> entities, Action<TEntity, TPrimaryKey> setPrimaryKey, int? commandTimeout = null);

        /// <summary>
        /// Updates the <paramref name="entity"/> in the databaseConnection.
        /// </summary>
        /// <example>
        /// <code>
        /// <![CDATA[
        /// var entity = databaseConnection.Get<DogEntity>(5);
        /// entity.Name = "Little bobby tables";
        /// databaseConnection.Update(entity);
        /// ]]>
        /// </code>
        /// </example>
        /// <exception cref="AffectedRowCountException">The update command didn't change any record, or changed multiple records.</exception>
        void Update<TEntity>(TEntity entity, int? commandTimeout = null, bool? verifyAffectedRowCount = null);

        /// <summary>
        /// <para>Efficiently updates multiple <paramref name="entities"/> in the databaseConnection.</para>
        /// <para>For performance, it's recommended to always perform this action inside of a transaction.</para>
        /// </summary>
        /// <example>
        /// <code>
        /// <![CDATA[
        /// using (var databaseConnection = databaseProvider.StartUnitOfWork())
        /// {
        ///     var entities = databaseConnection.GetRange<DogEntity>("WHERE @Age = 10");
        ///
        ///     foreach (var entity in entities)
        ///     {
        ///         entity.Name = "Little bobby tables";
        ///     }
        ///
        ///     databaseConnection.UpdateRange(entities);
        ///     databaseConnection.SaveChanges();
        /// }
        /// ]]>
        /// </code>
        /// </example>
        /// <returns>The number of affected records.</returns>
        CommandResult UpdateRange<TEntity>(IEnumerable<TEntity> entities, int? commandTimeout = null);

        /// <summary>
        /// Deletes the entity in the <typeparamref name="TEntity"/> table, identified by its primary key.
        /// </summary>
        /// <example>
        /// <code>
        /// <![CDATA[
        /// var entity = databaseConnection.Get<DogEntity>(5);
        /// databaseConnection.Delete(entity);
        /// ]]>
        /// </code>
        /// </example>
        /// <exception cref="AffectedRowCountException">The delete command didn't delete anything, or deleted multiple records.</exception>
        void Delete<TEntity>(TEntity entity, int? commandTimeout = null, bool? verifyAffectedRowCount = null);

        /// <summary>
        /// Deletes the entity in the <typeparamref name="TEntity"/> table which has the <paramref name="id"/>.
        /// </summary>
        /// <example>
        /// <code>
        /// databaseConnection.Delete(5);
        /// </code>
        /// </example>
        /// <exception cref="AffectedRowCountException">The delete command didn't delete anything, or deleted multiple records.</exception>
        void Delete<TEntity>(object id, int? commandTimeout = null, bool? verifyAffectedRowCount = null);

        /// <summary>
        /// <para>Deletes all the entities in the <typeparamref name="TEntity"/> table which match the <paramref name="conditions"/>.</para>
        /// <para>Note: <paramref name="conditions"/> must contain a WHERE clause. Use <see cref="DeleteAll{TEntity}"/> if you want to delete all entities.</para>
        /// </summary>
        /// <example>
        /// <code>
        /// <![CDATA[
        /// var searchTerm = "%Foo%";
        /// databaseConnection.DeleteRange<DogEntity>($"WHERE Name LIKE {searchTerm}");
        /// ]]>
        /// </code>
        /// </example>
        /// <returns>The number of deleted entities.</returns>
        CommandResult DeleteRange<TEntity>(FormattableString conditions, int? commandTimeout = null);

        /// <summary>
        /// <para>Deletes all the entities in the <typeparamref name="TEntity"/> table which match the <paramref name="conditions"/>.</para>
        /// <para>Note: <paramref name="conditions"/> must contain a WHERE clause. Use <see cref="DeleteAll{TEntity}"/> if you want to delete all entities.</para>
        /// </summary>
        /// <example>
        /// <code>
        /// <![CDATA[
        /// databaseConnection.DeleteRange<DogEntity>(new { Name = "Foo" });
        /// ]]>
        /// </code>
        /// </example>
        /// <returns>The number of deleted entities.</returns>
        CommandResult DeleteRange<TEntity>(object conditions, int? commandTimeout = null);

        /// <summary>
        /// Deletes all the entities in the <typeparamref name="TEntity"/> table.
        /// </summary>
        /// <example>
        /// <code>
        /// <![CDATA[
        /// databaseConnection.DeleteAll<DogEntity>();
        /// ]]>
        /// </code>
        /// </example>
        /// <returns>The number of deleted entities.</returns>
        CommandResult DeleteAll<TEntity>(int? commandTimeout = null);
    }
}