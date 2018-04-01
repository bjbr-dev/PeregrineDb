namespace PeregrineDb.Databases
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;
    using Pagination;
    using PeregrineDb.SqlCommands;
    using PeregrineDb.Utils;

    public partial class DefaultDatabaseConnection
    {
        public int Count<TEntity>(FormattableString conditions = null, int? commandTimeout = null)
        {
            return this.ExecuteScalar<int>(this.commandFactory.MakeCountCommand<TEntity>(conditions), commandTimeout);
        }

        public int Count<TEntity>(object conditions, int? commandTimeout = null)
        {
            return this.ExecuteScalar<int>(this.commandFactory.MakeCountCommand<TEntity>(conditions), commandTimeout);
        }

        public TEntity Find<TEntity>(object id, int? commandTimeout = null)
        {
            return this.QueryFirstOrDefault<TEntity>(this.commandFactory.MakeFindCommand<TEntity>(id), commandTimeout);
        }

        public TEntity Get<TEntity>(object id, int? commandTimeout = null)
            where TEntity : class
        {
            return this.Find<TEntity>(id, commandTimeout) ?? throw new InvalidOperationException($"An entity with id {id} was not found");
        }

        public TEntity GetFirstOrDefault<TEntity>(FormattableString conditions, string orderBy, int? commandTimeout = null)
        {
            return this.QueryFirstOrDefault<TEntity>(this.commandFactory.MakeGetTopNStatement<TEntity>(1, conditions, orderBy), commandTimeout);
        }

        public TEntity GetFirstOrDefault<TEntity>(object conditions, string orderBy, int? commandTimeout = null)
        {
            return this.QueryFirstOrDefault<TEntity>(this.commandFactory.MakeGetTopNStatement<TEntity>(1, conditions, orderBy), commandTimeout);
        }

        public TEntity GetFirst<TEntity>(FormattableString conditions, string orderBy, int? commandTimeout = null)
            where TEntity : class
        {
            var result = this.GetFirstOrDefault<TEntity>(conditions, orderBy, commandTimeout);
            return result ?? throw new InvalidOperationException($"No entity matching {conditions} was found");
        }

        public TEntity GetFirst<TEntity>(object conditions, string orderBy, int? commandTimeout = null)
            where TEntity : class
        {
            return this.QueryFirst<TEntity>(this.commandFactory.MakeGetTopNStatement<TEntity>(1, conditions, orderBy), commandTimeout);
        }

        public TEntity GetSingleOrDefault<TEntity>(FormattableString conditions, int? commandTimeout = null)
        {
            return this.QuerySingleOrDefault<TEntity>(this.commandFactory.MakeGetTopNStatement<TEntity>(2, conditions, null), commandTimeout);
        }

        public TEntity GetSingleOrDefault<TEntity>(object conditions, int? commandTimeout = null)
        {
            return this.QuerySingleOrDefault<TEntity>(this.commandFactory.MakeGetTopNStatement<TEntity>(2, conditions, null), commandTimeout);
        }

        public TEntity GetSingle<TEntity>(FormattableString conditions, int? commandTimeout = null)
            where TEntity : class
        {
            return this.QuerySingle<TEntity>(this.commandFactory.MakeGetTopNStatement<TEntity>(2, conditions, null), commandTimeout);
        }

        public TEntity GetSingle<TEntity>(object conditions, int? commandTimeout = null)
            where TEntity : class
        {
            return this.QuerySingle<TEntity>(this.commandFactory.MakeGetTopNStatement<TEntity>(2, conditions, null), commandTimeout);
        }

        public IEnumerable<TEntity> GetRange<TEntity>(FormattableString conditions, int? commandTimeout = null)
        {
            return this.Query<TEntity>(this.commandFactory.MakeGetRangeStatement<TEntity>(conditions), commandTimeout);
        }

        public IEnumerable<TEntity> GetRange<TEntity>(object conditions, int? commandTimeout = null)
        {
            return this.Query<TEntity>(this.commandFactory.MakeGetRangeStatement<TEntity>(conditions), commandTimeout);
        }

        public PagedList<TEntity> GetPage<TEntity>(IPageBuilder pageBuilder, FormattableString conditions, string orderBy, int? commandTimeout = null)
        {
            var totalNumberOfItems = this.Count<TEntity>(conditions, commandTimeout);
            var page = pageBuilder.GetCurrentPage(totalNumberOfItems);
            if (page.IsEmpty)
            {
                return PagedList<TEntity>.Empty(totalNumberOfItems, page);
            }

            var pageSql = this.commandFactory.MakeGetPageStatement<TEntity>(page, conditions, orderBy);
            var items = this.Query<TEntity>(pageSql, commandTimeout);
            return PagedList<TEntity>.Create(totalNumberOfItems, page, items);
        }

        public PagedList<TEntity> GetPage<TEntity>(IPageBuilder pageBuilder, object conditions, string orderBy, int? commandTimeout = null)
        {
            var totalNumberOfItems = this.Count<TEntity>(conditions, commandTimeout);
            var page = pageBuilder.GetCurrentPage(totalNumberOfItems);
            if (page.IsEmpty)
            {
                return PagedList<TEntity>.Empty(totalNumberOfItems, page);
            }

            var pageSql = this.commandFactory.MakeGetPageStatement<TEntity>(page, conditions, orderBy);
            var items = this.Query<TEntity>(pageSql, commandTimeout);
            return PagedList<TEntity>.Create(totalNumberOfItems, page, items);
        }

        public IEnumerable<TEntity> GetAll<TEntity>(int? commandTimeout = null)
        {
            return this.Query<TEntity>(this.commandFactory.MakeGetAllStatement<TEntity>(), commandTimeout);
        }

        public void Insert(object entity, int? commandTimeout = null, bool? verifyAffectedRowCount = null)
        {
            var result = this.Execute(this.commandFactory.MakeInsertStatement(entity), commandTimeout);
            if (this.Config.ShouldVerifyAffectedRowCount(verifyAffectedRowCount))
            {
                result.ExpectingAffectedRowCountToBe(1);
            }
        }

        public TPrimaryKey Insert<TPrimaryKey>(object entity, int? commandTimeout = null)
        {
            return this.ExecuteScalar<TPrimaryKey>(this.commandFactory.MakeInsertReturningPrimaryKeyStatement<TPrimaryKey>(entity), commandTimeout);
        }

        public CommandResult InsertRange<TEntity>(IEnumerable<TEntity> entities, int? commandTimeout = null)
        {
            var num = 0;
            foreach (var entity in entities)
            {
                this.Insert(entity, commandTimeout, false);
                num++;
            }

            return new CommandResult(num);
        }

        [SuppressMessage("ReSharper", "PossibleMultipleEnumeration", Justification = "Ensure doesn't enumerate")]
        public void InsertRange<TEntity, TPrimaryKey>(IEnumerable<TEntity> entities, Action<TEntity, TPrimaryKey> setPrimaryKey, int? commandTimeout = null)
        {
            Ensure.NotNull(setPrimaryKey, nameof(setPrimaryKey));
            Ensure.NotNull(entities, nameof(entities));

            foreach (var entity in entities)
            {
                var sql = this.commandFactory.MakeInsertReturningPrimaryKeyStatement<TPrimaryKey>(entity);
                var id = this.ExecuteScalar<TPrimaryKey>(sql, commandTimeout);
                setPrimaryKey(entity, id);
            }
        }

        public void Update<TEntity>(TEntity entity, int? commandTimeout = null, bool? verifyAffectedRowCount = null)
        {
            var result = this.Execute(this.commandFactory.MakeUpdateStatement<TEntity>(entity), commandTimeout);

            if (this.Config.ShouldVerifyAffectedRowCount(verifyAffectedRowCount))
            {
                result.ExpectingAffectedRowCountToBe(1);
            }
        }

        public CommandResult UpdateRange<TEntity>(IEnumerable<TEntity> entities, int? commandTimeout = null)
        {
            var num = 0;
            foreach (var entity in entities)
            {
                this.Update(entity, commandTimeout, false);
                num++;
            }

            return new CommandResult(num);
        }

        public void Delete<TEntity>(TEntity entity, int? commandTimeout = null, bool? verifyAffectedRowCount = null)
        {
            var result = this.Execute(this.commandFactory.MakeDeleteStatement<TEntity>(entity), commandTimeout);

            if (this.Config.ShouldVerifyAffectedRowCount(verifyAffectedRowCount))
            {
                result.ExpectingAffectedRowCountToBe(1);
            }
        }

        public void Delete<TEntity>(object id, int? commandTimeout = null, bool? verifyAffectedRowCount = null)
        {
            var result = this.Execute(this.commandFactory.MakeDeleteByPrimaryKeyStatement<TEntity>(id), commandTimeout);

            if (this.Config.ShouldVerifyAffectedRowCount(verifyAffectedRowCount))
            {
                result.ExpectingAffectedRowCountToBe(1);
            }
        }

        public CommandResult DeleteRange<TEntity>(FormattableString conditions, int? commandTimeout = null)
        {
            return this.Execute(this.commandFactory.MakeDeleteRangeStatement<TEntity>(conditions), commandTimeout);
        }

        public CommandResult DeleteRange<TEntity>(object conditions, int? commandTimeout = null)
        {
            return this.Execute(this.commandFactory.MakeDeleteRangeStatement<TEntity>(conditions), commandTimeout);
        }

        public CommandResult DeleteAll<TEntity>(int? commandTimeout = null)
        {
            return this.Execute(this.commandFactory.MakeDeleteAllStatement<TEntity>(), commandTimeout);
        }
    }
}