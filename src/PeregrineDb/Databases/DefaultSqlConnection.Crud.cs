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
        public int Count<TEntity>(FormattableString conditions = null, int? commandTimeout = null)
        {
            var command = this.Dialect.MakeCountCommand<TEntity>(conditions);
            return this.RawExecuteScalar<int>(command.CommandText, command.Parameters, CommandType.Text, commandTimeout);
        }

        public int Count<TEntity>(object conditions, int? commandTimeout = null)
        {
            var command = this.Dialect.MakeCountCommand<TEntity>(conditions);
            return this.RawExecuteScalar<int>(command.CommandText, command.Parameters, CommandType.Text, commandTimeout);
        }

        public bool Exists<TEntity>(FormattableString conditions = null, int? commandTimeout = null)
        {
            return this.Count<TEntity>(conditions, commandTimeout) > 0;
        }

        public bool Exists<TEntity>(object conditions, int? commandTimeout = null)
        {
            return this.Count<TEntity>(conditions, commandTimeout) > 0;
        }

        public TEntity Find<TEntity>(object id, int? commandTimeout = null)
        {
            var command = this.Dialect.MakeFindCommand<TEntity>(id);
            return this.RawQueryFirstOrDefault<TEntity>(command.CommandText, command.Parameters, CommandType.Text, commandTimeout);
        }

        public TEntity Get<TEntity>(object id, int? commandTimeout = null)
            where TEntity : class
        {
            return this.Find<TEntity>(id, commandTimeout) ?? throw new InvalidOperationException($"An entity with id {id} was not found");
        }

        public TEntity FindFirst<TEntity>(FormattableString conditions, string orderBy, int? commandTimeout = null)
        {
            var command = this.Dialect.MakeGetFirstNCommand<TEntity>(1, conditions, orderBy);
            return this.RawQueryFirstOrDefault<TEntity>(command.CommandText, command.Parameters, CommandType.Text, commandTimeout);
        }

        public TEntity FindFirst<TEntity>(object conditions, string orderBy, int? commandTimeout = null)
        {
            var command = this.Dialect.MakeGetFirstNCommand<TEntity>(1, conditions, orderBy);
            return this.RawQueryFirstOrDefault<TEntity>(command.CommandText, command.Parameters, CommandType.Text, commandTimeout);
        }

        public TEntity GetFirst<TEntity>(FormattableString conditions, string orderBy, int? commandTimeout = null)
            where TEntity : class
        {
            var result = this.FindFirst<TEntity>(conditions, orderBy, commandTimeout);
            return result ?? throw new InvalidOperationException($"No entity matching {conditions} was found");
        }

        public TEntity GetFirst<TEntity>(object conditions, string orderBy, int? commandTimeout = null)
            where TEntity : class
        {
            var command = this.Dialect.MakeGetFirstNCommand<TEntity>(1, conditions, orderBy);
            return this.RawQueryFirst<TEntity>(command.CommandText, command.Parameters, CommandType.Text, commandTimeout);
        }

        public TEntity FindSingle<TEntity>(FormattableString conditions, int? commandTimeout = null)
        {
            var command = this.Dialect.MakeGetFirstNCommand<TEntity>(2, conditions, null);
            return this.RawQuerySingleOrDefault<TEntity>(command.CommandText, command.Parameters, CommandType.Text, commandTimeout);
        }

        public TEntity FindSingle<TEntity>(object conditions, int? commandTimeout = null)
        {
            var command = this.Dialect.MakeGetFirstNCommand<TEntity>(2, conditions, null);
            return this.RawQuerySingleOrDefault<TEntity>(command.CommandText, command.Parameters, CommandType.Text, commandTimeout);
        }

        public TEntity GetSingle<TEntity>(FormattableString conditions, int? commandTimeout = null)
            where TEntity : class
        {
            var command = this.Dialect.MakeGetFirstNCommand<TEntity>(2, conditions, null);
            return this.RawQuerySingle<TEntity>(command.CommandText, command.Parameters, CommandType.Text, commandTimeout);
        }

        public TEntity GetSingle<TEntity>(object conditions, int? commandTimeout = null)
            where TEntity : class
        {
            var command = this.Dialect.MakeGetFirstNCommand<TEntity>(2, conditions, null);
            return this.RawQuerySingle<TEntity>(command.CommandText, command.Parameters, CommandType.Text, commandTimeout);
        }

        public IReadOnlyList<TEntity> GetRange<TEntity>(FormattableString conditions, int? commandTimeout = null)
        {
            var command = this.Dialect.MakeGetRangeCommand<TEntity>(conditions);
            return this.RawQuery<TEntity>(command.CommandText, command.Parameters, CommandType.Text, commandTimeout);
        }

        public IReadOnlyList<TEntity> GetRange<TEntity>(object conditions, int? commandTimeout = null)
        {
            var command = this.Dialect.MakeGetRangeCommand<TEntity>(conditions);
            return this.RawQuery<TEntity>(command.CommandText, command.Parameters, CommandType.Text, commandTimeout);
        }

        public IReadOnlyList<TEntity> GetTop<TEntity>(int count, string orderBy, int? commandTimeout = null)
        {
            Ensure.NotNullOrWhiteSpace(orderBy, nameof(orderBy));

            var command = this.Dialect.MakeGetFirstNCommand<TEntity>(count, orderBy);
            return this.RawQuery<TEntity>(command.CommandText, command.Parameters, CommandType.Text, commandTimeout);
        }

        public IReadOnlyList<TEntity> GetTop<TEntity>(int count, FormattableString conditions, string orderBy, int? commandTimeout = null)
        {
            Ensure.NotNullOrWhiteSpace(orderBy, nameof(orderBy));

            var command = this.Dialect.MakeGetFirstNCommand<TEntity>(count, conditions, orderBy);
            return this.RawQuery<TEntity>(command.CommandText, command.Parameters, CommandType.Text, commandTimeout);
        }

        public IReadOnlyList<TEntity> GetTop<TEntity>(int count, object conditions, string orderBy, int? commandTimeout = null)
        {
            Ensure.NotNullOrWhiteSpace(orderBy, nameof(orderBy));

            var command = this.Dialect.MakeGetFirstNCommand<TEntity>(count, conditions, orderBy);
            return this.RawQuery<TEntity>(command.CommandText, command.Parameters, CommandType.Text, commandTimeout);
        }

        public PagedList<TEntity> GetPage<TEntity>(IPageBuilder pageBuilder, FormattableString conditions, string orderBy, int? commandTimeout = null)
        {
            var countCommand = this.Dialect.MakeCountCommand<TEntity>(conditions);
            var totalNumberOfItems = this.RawExecuteScalar<int>(countCommand.CommandText, countCommand.Parameters, CommandType.Text, commandTimeout);
            var page = pageBuilder.GetCurrentPage(totalNumberOfItems);
            if (page.IsEmpty)
            {
                return PagedList<TEntity>.Empty(totalNumberOfItems, page);
            }

            var pageCommand = this.Dialect.MakeGetPageCommand<TEntity>(page, conditions, orderBy);
            var items = this.RawQuery<TEntity>(pageCommand.CommandText, pageCommand.Parameters, CommandType.Text, commandTimeout);
            return PagedList<TEntity>.Create(totalNumberOfItems, page, items);
        }

        public PagedList<TEntity> GetPage<TEntity>(IPageBuilder pageBuilder, object conditions, string orderBy, int? commandTimeout = null)
        {
            var countCommand = this.Dialect.MakeCountCommand<TEntity>(conditions);
            var totalNumberOfItems = this.RawExecuteScalar<int>(countCommand.CommandText, countCommand.Parameters, CommandType.Text, commandTimeout);
            var page = pageBuilder.GetCurrentPage(totalNumberOfItems);
            if (page.IsEmpty)
            {
                return PagedList<TEntity>.Empty(totalNumberOfItems, page);
            }

            var pageCommand = this.Dialect.MakeGetPageCommand<TEntity>(page, conditions, orderBy);
            var items = this.RawQuery<TEntity>(pageCommand.CommandText, pageCommand.Parameters, CommandType.Text, commandTimeout);
            return PagedList<TEntity>.Create(totalNumberOfItems, page, items);
        }

        public IReadOnlyList<TEntity> GetAll<TEntity>(int? commandTimeout = null)
        {
            var command = this.Dialect.MakeGetRangeCommand<TEntity>(null);
            return this.RawQuery<TEntity>(command.CommandText, command.Parameters, CommandType.Text, commandTimeout);
        }

        public void Insert(object entity, int? commandTimeout = null, bool? verifyAffectedRowCount = null)
        {
            var command = this.Dialect.MakeInsertCommand(entity);
            var result = this.RawExecute(command.CommandText, command.Parameters, CommandType.Text, commandTimeout);
            if (this.Config.ShouldVerifyAffectedRowCount(verifyAffectedRowCount))
            {
                result.ExpectingAffectedRowCountToBe(1);
            }
        }

        public TPrimaryKey Insert<TPrimaryKey>(object entity, int? commandTimeout = null)
        {
            var command = this.Dialect.MakeInsertReturningPrimaryKeyCommand<TPrimaryKey>(entity);
            return this.RawExecuteScalar<TPrimaryKey>(command.CommandText, command.Parameters, CommandType.Text, commandTimeout);
        }

        public CommandResult InsertRange<TEntity>(IEnumerable<TEntity> entities, int? commandTimeout = null)
        {
            var (sql, parameters) = this.Dialect.MakeInsertRangeCommand(entities);
            return this.RawExecuteMultiple(sql, parameters, CommandType.Text, commandTimeout);
        }

        [SuppressMessage("ReSharper", "PossibleMultipleEnumeration", Justification = "Ensure doesn't enumerate")]
        public void InsertRange<TEntity, TPrimaryKey>(IEnumerable<TEntity> entities, Action<TEntity, TPrimaryKey> setPrimaryKey, int? commandTimeout = null)
        {
            Ensure.NotNull(setPrimaryKey, nameof(setPrimaryKey));
            Ensure.NotNull(entities, nameof(entities));

            foreach (var entity in entities)
            {
                var command = this.Dialect.MakeInsertReturningPrimaryKeyCommand<TPrimaryKey>(entity);
                var id = this.RawExecuteScalar<TPrimaryKey>(command.CommandText, command.Parameters, CommandType.Text, commandTimeout);
                setPrimaryKey(entity, id);
            }
        }

        public void Update<TEntity>(TEntity entity, int? commandTimeout = null, bool? verifyAffectedRowCount = null)
            where TEntity : class
        {
            var command = this.Dialect.MakeUpdateCommand(entity);
            var result = this.RawExecute(command.CommandText, command.Parameters, CommandType.Text, commandTimeout);

            if (this.Config.ShouldVerifyAffectedRowCount(verifyAffectedRowCount))
            {
                result.ExpectingAffectedRowCountToBe(1);
            }
        }

        public CommandResult UpdateRange<TEntity>(IEnumerable<TEntity> entities, int? commandTimeout = null)
            where TEntity : class
        {
            var (sql, parameters) = this.Dialect.MakeUpdateRangeCommand(entities);
            return this.RawExecuteMultiple(sql, parameters, CommandType.Text, commandTimeout);
        }

        public void Delete<TEntity>(TEntity entity, int? commandTimeout = null, bool? verifyAffectedRowCount = null)
            where TEntity : class
        {
            var command = this.Dialect.MakeDeleteCommand(entity);
            var result = this.RawExecute(command.CommandText, command.Parameters, CommandType.Text, commandTimeout);

            if (this.Config.ShouldVerifyAffectedRowCount(verifyAffectedRowCount))
            {
                result.ExpectingAffectedRowCountToBe(1);
            }
        }

        public void Delete<TEntity>(object id, int? commandTimeout = null, bool? verifyAffectedRowCount = null)
        {
            var command = this.Dialect.MakeDeleteByPrimaryKeyCommand<TEntity>(id);
            var result = this.RawExecute(command.CommandText, command.Parameters, CommandType.Text, commandTimeout);

            if (this.Config.ShouldVerifyAffectedRowCount(verifyAffectedRowCount))
            {
                result.ExpectingAffectedRowCountToBe(1);
            }
        }

        public CommandResult DeleteRange<TEntity>(FormattableString conditions, int? commandTimeout = null)
        {
            var command = this.Dialect.MakeDeleteRangeCommand<TEntity>(conditions);
            return this.RawExecute(command.CommandText, command.Parameters, CommandType.Text, commandTimeout);
        }

        public CommandResult DeleteRange<TEntity>(object conditions, int? commandTimeout = null)
        {
            var command = this.Dialect.MakeDeleteRangeCommand<TEntity>(conditions);
            return this.RawExecute(command.CommandText, command.Parameters, CommandType.Text, commandTimeout);
        }

        public CommandResult DeleteAll<TEntity>(int? commandTimeout = null)
        {
            var command = this.Dialect.MakeDeleteAllCommand<TEntity>();
            return this.RawExecute(command.CommandText, command.Parameters, CommandType.Text, commandTimeout);
        }
    }
}