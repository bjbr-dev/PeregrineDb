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
        /// Counts all the entities in the <typeparamref name="TEntity"/> table which match the <paramref name="conditions"/>.
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
        /// var entity = this.connection.Find<User>(5);
        /// this.connection.Count<User>("WHERE Age > @MinAge", new { MinAge = 18 });
        /// ]]>
        /// </code>
        /// </example>
        public static int Count<TEntity>(
            this IDbConnection connection,
            string conditions = null,
            object parameters = null,
            IDbTransaction transaction = null,
            int? commandTimeout = null)
        {
            var dialect = MicroCRUDConfig.CurrentDialect;
            var tableSchema = TableSchemaFactory.GetTableSchema(typeof(TEntity), dialect);
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
        /// var entity = this.connection.Find<User>(12);
        /// ]]>
        /// </code>
        /// </example>
        public static TEntity Find<TEntity>(
            this IDbConnection connection,
            object id,
            IDbTransaction transaction = null,
            int? commandTimeout = null)
        {
            var dialect = MicroCRUDConfig.CurrentDialect;
            var tableSchema = TableSchemaFactory.GetTableSchema(typeof(TEntity), dialect);

            var primaryKey = tableSchema.GetSinglePrimaryKey("Find<TEntity>");

            var sql = SqlFactory.MakeFindStatement(tableSchema, primaryKey, dialect);
            var parameters = new DynamicParameters();
            parameters.Add("@id", id);

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
        /// var users = this.connection.GetRange<User>("WHERE Age > @MinAge", new { MinAge = 18 });
        /// ]]>
        /// </code>
        /// </example>
        public static IEnumerable<TEntity> GetRange<TEntity>(
            this IDbConnection connection,
            string conditions,
            object parameters = null,
            IDbTransaction transaction = null,
            int? commandTimeout = null)
        {
            var dialect = MicroCRUDConfig.CurrentDialect;
            var tableSchema = TableSchemaFactory.GetTableSchema(typeof(TEntity), dialect);
            var sql = SqlFactory.MakeGetRangeStatement(tableSchema, conditions, dialect);

            return connection.Query<TEntity>(sql, parameters, transaction, commandTimeout: commandTimeout);
        }

        /// <summary>
        /// Gets all the entities in the <typeparamref name="TEntity"/> table.
        /// </summary>
        public static IEnumerable<TEntity> GetAll<TEntity>(
            this IDbConnection connection,
            IDbTransaction transaction = null,
            int? commandTimeout = null)
        {
            var dialect = MicroCRUDConfig.CurrentDialect;
            var tableSchema = TableSchemaFactory.GetTableSchema(typeof(TEntity), dialect);
            var sql = SqlFactory.MakeGetRangeStatement(tableSchema, null, dialect);

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
            int? commandTimeout = null)
        {
            var dialect = MicroCRUDConfig.CurrentDialect;
            var tableSchema = TableSchemaFactory.GetTableSchema(entity.GetType(), dialect);

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
            int? commandTimeout = null)
        {
            var tableSchema = TableSchemaFactory.GetTableSchema(entity.GetType(), MicroCRUDConfig.CurrentDialect);
            tableSchema.GetSinglePrimaryKey("Insert");

            var keyType = typeof(TPrimaryKey).GetUnderlyingType();
            if (keyType != typeof(int) && keyType != typeof(long))
            {
                throw new NotSupportedException("Entities can only have an Int32 or Int64 Key.");
            }

            var sql = SqlFactory.MakeInsertReturningIdentityStatement(tableSchema, MicroCRUDConfig.CurrentDialect);
            return connection.Query<TPrimaryKey>(sql, entity, transaction, commandTimeout: commandTimeout).Single();
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
        /// var entity = this.connection.Find<User>(5);
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
            int? commandTimeout = null)
        {
            var tableSchema = TableSchemaFactory.GetTableSchema(typeof(TEntity), MicroCRUDConfig.CurrentDialect);
            var primaryKey = tableSchema.GetSinglePrimaryKey("Insert");

            var sql = SqlFactory.MakeUpdateStatement(tableSchema, primaryKey);
            return connection.Execute(sql, entity, transaction, commandTimeout);
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
        /// var entity = this.connection.Find<User>(5);
        /// this.connection.Delete(entity);
        /// ]]>
        /// </code>
        /// </example>
        /// <returns>The number of affected records.</returns>
        public static int Delete<TEntity>(
            this IDbConnection connection,
            TEntity entity,
            IDbTransaction transaction = null,
            int? commandTimeout = null)
        {
            var tableSchema = TableSchemaFactory.GetTableSchema(typeof(TEntity), MicroCRUDConfig.CurrentDialect);
            var primaryKey = tableSchema.GetSinglePrimaryKey("Delete<TEntity>");

            var sql = SqlFactory.MakeDeleteByPrimaryKeyStatement(tableSchema, primaryKey);

            return connection.Execute(sql, entity, transaction, commandTimeout);
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
        /// <returns>The number of affected records.</returns>
        public static int Delete<TEntity>(
            this IDbConnection connection,
            object id,
            IDbTransaction transaction = null,
            int? commandTimeout = null)
        {
            var tableSchema = TableSchemaFactory.GetTableSchema(typeof(TEntity), MicroCRUDConfig.CurrentDialect);
            var primaryKey = tableSchema.GetSinglePrimaryKey("Delete<TEntity>");

            var sql = SqlFactory.MakeDeleteByPrimaryKeyStatement(tableSchema, primaryKey);

            var parameters = new DynamicParameters();
            parameters.Add("@" + tableSchema.GetSinglePrimaryKey("Delete<TEntity>").PropertyName, id);

            return connection.Execute(sql, parameters, transaction, commandTimeout);
        }

        /// <summary>
        /// Deletes all the entities in the <typeparamref name="TEntity"/> table which match the <paramref name="conditions"/>.
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
        /// var entity = this.connection.Find<User>(5);
        /// this.connection.Delete(entity);
        /// ]]>
        /// </code>
        /// </example>
        /// <returns>The number of affected records.</returns>
        public static int DeleteRange<TEntity>(
            this IDbConnection connection,
            string conditions,
            object parameters = null,
            IDbTransaction transaction = null,
            int? commandTimeout = null)
        {
            if (conditions == null || conditions.IndexOf("WHERE ", StringComparison.OrdinalIgnoreCase) < 0)
            {
                throw new ArgumentException("DeleteRange<T> requires a WHERE clause");
            }

            var tableSchema = TableSchemaFactory.GetTableSchema(typeof(TEntity), MicroCRUDConfig.CurrentDialect);
            var sql = SqlFactory.MakeDeleteRangeStatement(tableSchema, conditions);
            return connection.Execute(sql, parameters, transaction, commandTimeout);
        }
    }
}