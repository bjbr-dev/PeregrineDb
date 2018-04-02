namespace PeregrineDb
{
    using System.Collections.Generic;

    public partial interface ISqlConnection
    {
        /// <summary>
        /// <para>Creates a temporary table and inserts the <paramref name="entities"/>.</para>
        /// <para>For performance, it's recommended to always perform this action inside of a transaction.</para>
        /// </summary>
        /// <example>
        /// <code>
        /// <![CDATA[
        /// var entities = new []
        ///     {
        ///         new TempUser { Name = "Little bobby tables" },
        ///         new TempUser { Name = "Jimmy" };
        ///     };
        ///
        /// using (var databaseConnection = this.databaseProvider.StartUnitOfWork())
        /// {
        ///     database.CreateTempTable(entities);
        ///     transaction.SaveChanges();
        /// }
        /// ]]>
        /// </code>
        /// </example>
        void CreateTempTable<TEntity>(IEnumerable<TEntity> entities, int? commandTimeout = null);

        /// <summary>
        /// <para>Drops the temporary table defined by the <typeparam name="TEntity"></typeparam>.</para>
        /// <para>USE WITH CAUTION! In some dialects, this can drop a non-temporary table! Make sure you specify the right entity.</para>
        /// </summary>
        /// <example>
        /// <code>
        /// <![CDATA[
        /// databaseConnection.DropTempTable<TempUser>();
        /// ]]>
        /// </code>
        /// </example>
        void DropTempTable<TEntity>(int? commandTimeout = null);
    }
}