// <copyright file="DbConnectionTempTableExtensions.cs" company="Berkeleybross">
// Copyright (c) Berkeleybross. All rights reserved.
// </copyright>
namespace Dapper.MicroCRUD
{
    using System.Collections.Generic;
    using System.Data;
    using Dapper.MicroCRUD.Dialects;
    using Dapper.MicroCRUD.SqlCommands;
    using Dapper.MicroCRUD.Utils;

    /// <summary>
    /// Bulk extensions to the <see cref="IDbConnection"/>.
    /// </summary>
    public static class DbConnectionTempTableExtensions
    {
        /// <summary>
        /// <para>Creates a temporary table and inserts the <paramref name="entities"/>.</para>
        /// <para>For performance, it's recommended to always perform this action inside of a transaction.</para>
        /// </summary>
        /// <example>
        /// <code>
        /// <![CDATA[
        /// [Table("TempUsers")]
        /// public class TempUserEntity
        /// {
        ///     [Key]
        ///     public int Id { get; set; }
        ///
        ///     public string Name { get; set; }
        ///
        ///     public int Age { get; set; }
        /// }
        /// ...
        /// var entities = new []
        ///     {
        ///         new TempUser { Name = "Little bobby tables" },
        ///         new TempUser { Name = "Jimmy" };
        ///     };
        ///
        /// using (var transaction = this.connection.BeginTransaction())
        /// {
        ///     this.connection.CreateTempTable(entities, transaction);
        ///
        ///     transaction.Commit();
        /// }
        /// ]]>
        /// </code>
        /// </example>
        public static void CreateTempTable<TEntity>(
            this IDbConnection connection,
            IEnumerable<TEntity> entities,
            IDbTransaction transaction = null,
            IDialect dialect = null,
            int? commandTimeout = null)
        {
            Ensure.NotNull(connection, nameof(connection));
            var command = CommandFactory.MakeCreateTempTableCommand<TEntity>(transaction, dialect, commandTimeout);
            connection.Execute(command);

            var insertCommnad = CommandFactory.MakeInsertRangeCommand(entities, transaction, dialect, commandTimeout);
            connection.Execute(insertCommnad);
        }

        public static void CreateTempTable<TEntity>(
            this IDapperConnection connection,
            IEnumerable<TEntity> entities,
            int? commandTimeout = null)
        {
            connection.DbConnection.CreateTempTable(entities, connection.Transaction, connection.Dialect, commandTimeout);
        }

        /// <summary>
        /// <para>Drops the temporary table with the given <paramref name="tableName"/>.</para>
        /// <para>USE WITH CAUTION! In some dialects, this can drop a non-temporary table! Make sure you specify the right entity and tablename.</para>
        /// </summary>
        /// <example>
        /// <code>
        /// <![CDATA[
        /// [Table("TempUsers")]
        /// public class TempUserEntity
        /// {
        ///     [Key]
        ///     public int Id { get; set; }
        ///
        ///     public string Name { get; set; }
        ///
        ///     public int Age { get; set; }
        /// }
        /// ...
        /// this.connection.DropTempTable<TempUser>("[TempUsers]");
        /// ]]>
        /// </code>
        /// </example>
        public static void DropTempTable<TEntity>(
            this IDbConnection connection,
            string tableName,
            IDbTransaction transaction = null,
            IDialect dialect = null,
            int? commandTimeout = null)
        {
            Ensure.NotNull(connection, nameof(connection));
            var command = CommandFactory.MakeDropTempTableCommand<TEntity>(tableName, transaction, dialect, commandTimeout);
            connection.Execute(command);
        }

        public static void DropTempTable<TEntity>(
            this IDapperConnection connection,
            string tableName,
            int? commandTimeout = null)
        {
            connection.DbConnection.DropTempTable<TEntity>(tableName, connection.Transaction, connection.Dialect, commandTimeout);
        }
    }
}