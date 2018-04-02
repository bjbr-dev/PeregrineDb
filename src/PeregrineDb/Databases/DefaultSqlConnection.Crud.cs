﻿namespace PeregrineDb.Databases
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;
    using Pagination;
    using PeregrineDb.SqlCommands;
    using PeregrineDb.Utils;

    public partial class DefaultSqlConnection
    {
        public int Count<TEntity>(FormattableString conditions = null, int? commandTimeout = null)
        {
            var command = this.commandFactory.MakeCountCommand<TEntity>(conditions);
            return this.ExecuteScalar<int>(in command, commandTimeout);
        }

        public int Count<TEntity>(object conditions, int? commandTimeout = null)
        {
            var command = this.commandFactory.MakeCountCommand<TEntity>(conditions);
            return this.ExecuteScalar<int>(in command, commandTimeout);
        }

        public TEntity Find<TEntity>(object id, int? commandTimeout = null)
        {
            var command = this.commandFactory.MakeFindCommand<TEntity>(id);
            return this.QueryFirstOrDefault<TEntity>(in command, commandTimeout);
        }

        public TEntity Get<TEntity>(object id, int? commandTimeout = null)
            where TEntity : class
        {
            return this.Find<TEntity>(id, commandTimeout) ?? throw new InvalidOperationException($"An entity with id {id} was not found");
        }

        public TEntity GetFirstOrDefault<TEntity>(FormattableString conditions, string orderBy, int? commandTimeout = null)
        {
            var command = this.commandFactory.MakeGetTopNCommand<TEntity>(1, conditions, orderBy);
            return this.QueryFirstOrDefault<TEntity>(in command, commandTimeout);
        }

        public TEntity GetFirstOrDefault<TEntity>(object conditions, string orderBy, int? commandTimeout = null)
        {
            var command = this.commandFactory.MakeGetTopNCommand<TEntity>(1, conditions, orderBy);
            return this.QueryFirstOrDefault<TEntity>(in command, commandTimeout);
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
            var command = this.commandFactory.MakeGetTopNCommand<TEntity>(1, conditions, orderBy);
            return this.QueryFirst<TEntity>(in command, commandTimeout);
        }

        public TEntity GetSingleOrDefault<TEntity>(FormattableString conditions, int? commandTimeout = null)
        {
            var command = this.commandFactory.MakeGetTopNCommand<TEntity>(2, conditions, null);
            return this.QuerySingleOrDefault<TEntity>(in command, commandTimeout);
        }

        public TEntity GetSingleOrDefault<TEntity>(object conditions, int? commandTimeout = null)
        {
            var command = this.commandFactory.MakeGetTopNCommand<TEntity>(2, conditions, null);
            return this.QuerySingleOrDefault<TEntity>(in command, commandTimeout);
        }

        public TEntity GetSingle<TEntity>(FormattableString conditions, int? commandTimeout = null)
            where TEntity : class
        {
            var command = this.commandFactory.MakeGetTopNCommand<TEntity>(2, conditions, null);
            return this.QuerySingle<TEntity>(in command, commandTimeout);
        }

        public TEntity GetSingle<TEntity>(object conditions, int? commandTimeout = null)
            where TEntity : class
        {
            var command = this.commandFactory.MakeGetTopNCommand<TEntity>(2, conditions, null);
            return this.QuerySingle<TEntity>(in command, commandTimeout);
        }

        public IEnumerable<TEntity> GetRange<TEntity>(FormattableString conditions, int? commandTimeout = null)
        {
            var command = this.commandFactory.MakeGetRangeCommand<TEntity>(conditions);
            return this.Query<TEntity>(in command, commandTimeout);
        }

        public IEnumerable<TEntity> GetRange<TEntity>(object conditions, int? commandTimeout = null)
        {
            var command = this.commandFactory.MakeGetRangeCommand<TEntity>(conditions);
            return this.Query<TEntity>(in command, commandTimeout);
        }

        public PagedList<TEntity> GetPage<TEntity>(IPageBuilder pageBuilder, FormattableString conditions, string orderBy, int? commandTimeout = null)
        {
            var countCommand = this.commandFactory.MakeCountCommand<TEntity>(conditions);
            var totalNumberOfItems = this.ExecuteScalar<int>(in countCommand, commandTimeout);
            var page = pageBuilder.GetCurrentPage(totalNumberOfItems);
            if (page.IsEmpty)
            {
                return PagedList<TEntity>.Empty(totalNumberOfItems, page);
            }

            var pageCommand = this.commandFactory.MakeGetPageCommand<TEntity>(page, conditions, orderBy);
            var items = this.Query<TEntity>(in pageCommand, commandTimeout);
            return PagedList<TEntity>.Create(totalNumberOfItems, page, items);
        }

        public PagedList<TEntity> GetPage<TEntity>(IPageBuilder pageBuilder, object conditions, string orderBy, int? commandTimeout = null)
        {
            var countCommand = this.commandFactory.MakeCountCommand<TEntity>(conditions);
            var totalNumberOfItems = this.ExecuteScalar<int>(in countCommand, commandTimeout);
            var page = pageBuilder.GetCurrentPage(totalNumberOfItems);
            if (page.IsEmpty)
            {
                return PagedList<TEntity>.Empty(totalNumberOfItems, page);
            }

            var pageCommand = this.commandFactory.MakeGetPageCommand<TEntity>(page, conditions, orderBy);
            var items = this.Query<TEntity>(in pageCommand, commandTimeout);
            return PagedList<TEntity>.Create(totalNumberOfItems, page, items);
        }

        public IEnumerable<TEntity> GetAll<TEntity>(int? commandTimeout = null)
        {
            var command = this.commandFactory.MakeGetAllCommand<TEntity>();
            return this.Query<TEntity>(in command, commandTimeout);
        }

        public void Insert(object entity, int? commandTimeout = null, bool? verifyAffectedRowCount = null)
        {
            var command = this.commandFactory.MakeInsertCommand(entity);
            var result = this.Execute(in command, commandTimeout);
            if (this.Config.ShouldVerifyAffectedRowCount(verifyAffectedRowCount))
            {
                result.ExpectingAffectedRowCountToBe(1);
            }
        }

        public TPrimaryKey Insert<TPrimaryKey>(object entity, int? commandTimeout = null)
        {
            var command = this.commandFactory.MakeInsertReturningPrimaryKeyCommand<TPrimaryKey>(entity);
            return this.ExecuteScalar<TPrimaryKey>(in command, commandTimeout);
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
                var command = this.commandFactory.MakeInsertReturningPrimaryKeyCommand<TPrimaryKey>(entity);
                var id = this.ExecuteScalar<TPrimaryKey>(in command, commandTimeout);
                setPrimaryKey(entity, id);
            }
        }

        public void Update<TEntity>(TEntity entity, int? commandTimeout = null, bool? verifyAffectedRowCount = null)
        {
            var command = this.commandFactory.MakeUpdateCommand<TEntity>(entity);
            var result = this.Execute(in command, commandTimeout);

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
            var command = this.commandFactory.MakeDeleteCommand<TEntity>(entity);
            var result = this.Execute(in command, commandTimeout);

            if (this.Config.ShouldVerifyAffectedRowCount(verifyAffectedRowCount))
            {
                result.ExpectingAffectedRowCountToBe(1);
            }
        }

        public void Delete<TEntity>(object id, int? commandTimeout = null, bool? verifyAffectedRowCount = null)
        {
            var command = this.commandFactory.MakeDeleteByPrimaryKeyCommand<TEntity>(id);
            var result = this.Execute(in command, commandTimeout);

            if (this.Config.ShouldVerifyAffectedRowCount(verifyAffectedRowCount))
            {
                result.ExpectingAffectedRowCountToBe(1);
            }
        }

        public CommandResult DeleteRange<TEntity>(FormattableString conditions, int? commandTimeout = null)
        {
            var command = this.commandFactory.MakeDeleteRangeCommand<TEntity>(conditions);
            return this.Execute(in command, commandTimeout);
        }

        public CommandResult DeleteRange<TEntity>(object conditions, int? commandTimeout = null)
        {
            var command = this.commandFactory.MakeDeleteRangeCommand<TEntity>(conditions);
            return this.Execute(in command, commandTimeout);
        }

        public CommandResult DeleteAll<TEntity>(int? commandTimeout = null)
        {
            var command = this.commandFactory.MakeDeleteAllCommand<TEntity>();
            return this.Execute(in command, commandTimeout);
        }
    }
}