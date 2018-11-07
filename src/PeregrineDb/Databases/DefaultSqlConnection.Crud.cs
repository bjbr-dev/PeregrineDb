// <copyright file="DefaultSqlConnection.Crud.cs" company="Berkeleybross">
// Copyright (c) Berkeleybross. All rights reserved.
// </copyright>
namespace PeregrineDb.Databases
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Diagnostics.CodeAnalysis;
    using Pagination;
    using PeregrineDb.SqlCommands;
    using PeregrineDb.Utils;

    public partial class DefaultSqlConnection
    {
        public int Count<TEntity>(string conditions = null, object parameters = null, int? commandTimeout = null)
        {
            var command = this.commandFactory.MakeCountCommand<TEntity>(conditions, parameters);
            return this.ExecuteScalar<int>(command.CommandText, command.Parameters, CommandType.Text, commandTimeout);
        }

        public int Count<TEntity>(object conditions, int? commandTimeout = null)
        {
            var command = this.commandFactory.MakeCountCommand<TEntity>(conditions);
            return this.ExecuteScalar<int>(command.CommandText, command.Parameters, CommandType.Text, commandTimeout);
        }

        public bool Exists<TEntity>(string conditions, object parameters, int? commandTimeout = null)
        {
            return this.Count<TEntity>(conditions, parameters, commandTimeout) > 0;
        }

        public bool Exists<TEntity>(object conditions, int? commandTimeout = null)
        {
            return this.Count<TEntity>(conditions, commandTimeout) > 0;
        }

        public TEntity Find<TEntity>(object id, int? commandTimeout = null)
        {
            var command = this.commandFactory.MakeFindCommand<TEntity>(id);
            return this.QueryFirstOrDefault<TEntity>(command.CommandText, command.Parameters, CommandType.Text, commandTimeout);
        }

        public TEntity Get<TEntity>(object id, int? commandTimeout = null)
            where TEntity : class
        {
            return this.Find<TEntity>(id, commandTimeout) ?? throw new InvalidOperationException($"An entity with id {id} was not found");
        }

        public TEntity FindFirst<TEntity>(string orderBy, string conditions, object parameters, int? commandTimeout = null)
        {
            var command = this.commandFactory.MakeGetFirstNCommand<TEntity>(1, conditions, parameters, orderBy);
            return this.QueryFirstOrDefault<TEntity>(command.CommandText, command.Parameters, CommandType.Text, commandTimeout);
        }

        public TEntity FindFirst<TEntity>(string orderBy, object conditions, int? commandTimeout = null)
        {
            var command = this.commandFactory.MakeGetFirstNCommand<TEntity>(1, conditions, orderBy);
            return this.QueryFirstOrDefault<TEntity>(command.CommandText, command.Parameters, CommandType.Text, commandTimeout);
        }

        public TEntity GetFirst<TEntity>(string orderBy, string conditions, object parameters, int? commandTimeout = null)
            where TEntity : class
        {
            var result = this.FindFirst<TEntity>(orderBy, conditions, parameters, commandTimeout);
            return result ?? throw new InvalidOperationException($"No entity matching {conditions} was found");
        }

        public TEntity GetFirst<TEntity>(string orderBy, object conditions, int? commandTimeout = null)
            where TEntity : class
        {
            var command = this.commandFactory.MakeGetFirstNCommand<TEntity>(1, conditions, orderBy);
            return this.QueryFirst<TEntity>(command.CommandText, command.Parameters, CommandType.Text, commandTimeout);
        }

        public TEntity FindSingle<TEntity>(string conditions, object parameters, int? commandTimeout = null)
        {
            var command = this.commandFactory.MakeGetFirstNCommand<TEntity>(2, conditions, parameters, null);
            return this.QuerySingleOrDefault<TEntity>(command.CommandText, command.Parameters, CommandType.Text, commandTimeout);
        }

        public TEntity FindSingle<TEntity>(object conditions, int? commandTimeout = null)
        {
            var command = this.commandFactory.MakeGetFirstNCommand<TEntity>(2, conditions, null);
            return this.QuerySingleOrDefault<TEntity>(command.CommandText, command.Parameters, CommandType.Text, commandTimeout);
        }

        public TEntity GetSingle<TEntity>(string conditions, object parameters, int? commandTimeout = null)
            where TEntity : class
        {
            var command = this.commandFactory.MakeGetFirstNCommand<TEntity>(2, conditions, parameters, null);
            return this.QuerySingle<TEntity>(command.CommandText, command.Parameters, CommandType.Text, commandTimeout);
        }

        public TEntity GetSingle<TEntity>(object conditions, int? commandTimeout = null)
            where TEntity : class
        {
            var command = this.commandFactory.MakeGetFirstNCommand<TEntity>(2, conditions, null);
            return this.QuerySingle<TEntity>(command.CommandText, command.Parameters, CommandType.Text, commandTimeout);
        }

        public IReadOnlyList<TEntity> GetRange<TEntity>(string conditions, object parameters, int? commandTimeout = null)
        {
            var command = this.commandFactory.MakeGetRangeCommand<TEntity>(conditions, parameters);
            return this.Query<TEntity>(command.CommandText, command.Parameters, CommandType.Text, commandTimeout);
        }

        public IReadOnlyList<TEntity> GetRange<TEntity>(object conditions, int? commandTimeout = null)
        {
            var command = this.commandFactory.MakeGetRangeCommand<TEntity>(conditions);
            return this.Query<TEntity>(command.CommandText, command.Parameters, CommandType.Text, commandTimeout);
        }

        public IReadOnlyList<TEntity> GetTop<TEntity>(int count, string orderBy, string conditions, object parameters, int? commandTimeout = null)
        {
            Ensure.NotNullOrWhiteSpace(orderBy, nameof(orderBy));

            var command = this.commandFactory.MakeGetFirstNCommand<TEntity>(count, conditions, parameters, orderBy);
            return this.Query<TEntity>(command.CommandText, command.Parameters, CommandType.Text, commandTimeout);
        }

        public IReadOnlyList<TEntity> GetTop<TEntity>(int count, string orderBy, object conditions, int? commandTimeout = null)
        {
            Ensure.NotNullOrWhiteSpace(orderBy, nameof(orderBy));

            var command = this.commandFactory.MakeGetFirstNCommand<TEntity>(count, conditions, orderBy);
            return this.Query<TEntity>(command.CommandText, command.Parameters, CommandType.Text, commandTimeout);
        }

        public PagedList<TEntity> GetPage<TEntity>(IPageBuilder pageBuilder, string orderBy, string conditions, object parameters, int? commandTimeout = null)
        {
            var totalNumberOfItems = this.Count<TEntity>(conditions, parameters, commandTimeout);
            var page = pageBuilder.GetCurrentPage(totalNumberOfItems);
            if (page.IsEmpty)
            {
                return PagedList<TEntity>.Empty(totalNumberOfItems, page);
            }

            var pageCommand = this.commandFactory.MakeGetPageCommand<TEntity>(page, conditions, parameters, orderBy);
            var items = this.Query<TEntity>(pageCommand.CommandText, pageCommand.Parameters, CommandType.Text, commandTimeout);
            return PagedList<TEntity>.Create(totalNumberOfItems, page, items);
        }

        public PagedList<TEntity> GetPage<TEntity>(IPageBuilder pageBuilder, string orderBy, object conditions, int? commandTimeout = null)
        {
            var totalNumberOfItems = this.Count<TEntity>(conditions, commandTimeout);
            var page = pageBuilder.GetCurrentPage(totalNumberOfItems);
            if (page.IsEmpty)
            {
                return PagedList<TEntity>.Empty(totalNumberOfItems, page);
            }

            var pageCommand = this.commandFactory.MakeGetPageCommand<TEntity>(page, conditions, orderBy);
            var items = this.Query<TEntity>(pageCommand.CommandText, pageCommand.Parameters, CommandType.Text, commandTimeout);
            return PagedList<TEntity>.Create(totalNumberOfItems, page, items);
        }

        public IReadOnlyList<TEntity> GetAll<TEntity>(int? commandTimeout = null)
        {
            var command = this.commandFactory.MakeGetRangeCommand<TEntity>(null, null);
            return this.Query<TEntity>(command.CommandText, command.Parameters, CommandType.Text, commandTimeout);
        }

        public void Insert(object entity, int? commandTimeout = null, bool? verifyAffectedRowCount = null)
        {
            var command = this.commandFactory.MakeInsertCommand(entity);
            var result = this.Execute(command.CommandText, command.Parameters, CommandType.Text, commandTimeout);
            if (this.Config.ShouldVerifyAffectedRowCount(verifyAffectedRowCount))
            {
                result.ExpectingAffectedRowCountToBe(1);
            }
        }

        public TPrimaryKey Insert<TPrimaryKey>(object entity, int? commandTimeout = null)
        {
            Ensure.NotNull(entity, nameof(entity));

            var command = this.commandFactory.MakeInsertReturningPrimaryKeyCommand<TPrimaryKey>(entity);
            return this.ExecuteScalar<TPrimaryKey>(command.CommandText, command.Parameters, CommandType.Text, commandTimeout);
        }

        public CommandResult InsertRange<TEntity>(IEnumerable<TEntity> entities, int? commandTimeout = null)
        {
            var command = this.commandFactory.MakeInsertRangeCommand(entities);
            return this.ExecuteMultiple(command.CommandText, command.Parameters, CommandType.Text, commandTimeout);
        }

        [SuppressMessage("ReSharper", "PossibleMultipleEnumeration", Justification = "Ensure doesn't enumerate")]
        public void InsertRange<TEntity, TPrimaryKey>(IEnumerable<TEntity> entities, Action<TEntity, TPrimaryKey> setPrimaryKey, int? commandTimeout = null)
        {
            Ensure.NotNull(setPrimaryKey, nameof(setPrimaryKey));
            Ensure.NotNull(entities, nameof(entities));

            foreach (var entity in entities)
            {
                var command = this.commandFactory.MakeInsertReturningPrimaryKeyCommand<TPrimaryKey>(entity);
                var id = this.ExecuteScalar<TPrimaryKey>(command.CommandText, command.Parameters, CommandType.Text, commandTimeout);
                setPrimaryKey(entity, id);
            }
        }

        public void Update<TEntity>(TEntity entity, int? commandTimeout = null, bool? verifyAffectedRowCount = null)
            where TEntity : class
        {
            var command = this.commandFactory.MakeUpdateCommand(entity);
            var result = this.Execute(command.CommandText, command.Parameters, CommandType.Text, commandTimeout);

            if (this.Config.ShouldVerifyAffectedRowCount(verifyAffectedRowCount))
            {
                result.ExpectingAffectedRowCountToBe(1);
            }
        }

        public CommandResult UpdateRange<TEntity>(IEnumerable<TEntity> entities, int? commandTimeout = null)
            where TEntity : class
        {
            var command = this.commandFactory.MakeUpdateRangeCommand(entities);
            return this.ExecuteMultiple(command.CommandText, command.Parameters, CommandType.Text, commandTimeout);
        }

        public void Delete<TEntity>(TEntity entity, int? commandTimeout = null, bool? verifyAffectedRowCount = null)
            where TEntity : class
        {
            var command = this.commandFactory.MakeDeleteCommand(entity);
            var result = this.Execute(command.CommandText, command.Parameters, CommandType.Text, commandTimeout);

            if (this.Config.ShouldVerifyAffectedRowCount(verifyAffectedRowCount))
            {
                result.ExpectingAffectedRowCountToBe(1);
            }
        }

        public void Delete<TEntity>(object id, int? commandTimeout = null, bool? verifyAffectedRowCount = null)
        {
            var command = this.commandFactory.MakeDeleteByPrimaryKeyCommand<TEntity>(id);
            var result = this.Execute(command.CommandText, command.Parameters, CommandType.Text, commandTimeout);

            if (this.Config.ShouldVerifyAffectedRowCount(verifyAffectedRowCount))
            {
                result.ExpectingAffectedRowCountToBe(1);
            }
        }

        public CommandResult DeleteRange<TEntity>(string conditions, object parameters, int? commandTimeout = null)
        {
            var command = this.commandFactory.MakeDeleteRangeCommand<TEntity>(conditions, parameters);
            return this.Execute(command.CommandText, command.Parameters, CommandType.Text, commandTimeout);
        }

        public CommandResult DeleteRange<TEntity>(object conditions, int? commandTimeout = null)
        {
            var command = this.commandFactory.MakeDeleteRangeCommand<TEntity>(conditions);
            return this.Execute(command.CommandText, command.Parameters, CommandType.Text, commandTimeout);
        }

        public CommandResult DeleteAll<TEntity>(int? commandTimeout = null)
        {
            var command = this.commandFactory.MakeDeleteAllCommand<TEntity>();
            return this.Execute(command.CommandText, command.Parameters, CommandType.Text, commandTimeout);
        }
    }
}