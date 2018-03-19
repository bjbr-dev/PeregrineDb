namespace PeregrineDb.Databases
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;
    using System.Linq;
    using Dapper;
    using Pagination;
    using PeregrineDb.SqlCommands;
    using PeregrineDb.Utils;

    public partial class DefaultDatabaseConnection
    {
        public int Count<TEntity>(string conditions = null, object parameters = null, int? commandTimeout = null)
        {
            var command = this.commandFactory.MakeCountCommand<TEntity>(conditions, parameters, commandTimeout);
            return this.DbConnection.ExecuteScalar<int>(command);
        }
        
        public int Count<TEntity>(object conditions, int? commandTimeout = null)
        {
            var command = this.commandFactory.MakeCountCommand<TEntity>(conditions, commandTimeout);
            return this.DbConnection.ExecuteScalar<int>(command);
        }

        public TEntity Find<TEntity>(object id, int? commandTimeout = null)
        {
            var command = this.commandFactory.MakeFindCommand<TEntity>(id, commandTimeout);
            return this.DbConnection.Query<TEntity>(command).FirstOrDefault();
        }

        public TEntity Get<TEntity>(object id, int? commandTimeout = null)
            where TEntity : class
        {
            return this.Find<TEntity>(id, commandTimeout) ?? throw new InvalidOperationException($"An entity with id {id} was not found");
        }

        public TEntity GetFirstOrDefault<TEntity>(string conditions, string orderBy, object parameters = null, int? commandTimeout = null)
        {
            var command = this.commandFactory.MakeGetTopNCommand<TEntity>(1, conditions, orderBy, parameters, commandTimeout);
            return this.DbConnection.Query<TEntity>(command).FirstOrDefault();
        }

        public TEntity GetFirstOrDefault<TEntity>(object conditions, string orderBy, int? commandTimeout = null)
        {
            var command = this.commandFactory.MakeGetTopNCommand<TEntity>(1, conditions, orderBy, commandTimeout);
            return this.DbConnection.Query<TEntity>(command).FirstOrDefault();
        }
        
        public TEntity GetFirst<TEntity>(string conditions, string orderBy, object parameters = null, int? commandTimeout = null)
            where TEntity : class
        {
            var result = this.GetFirstOrDefault<TEntity>(conditions, orderBy, parameters, commandTimeout);
            return result ?? throw new InvalidOperationException($"No entity matching {conditions} was found");
        }

        public TEntity GetFirst<TEntity>(object conditions, string orderBy, int? commandTimeout = null)
            where TEntity : class
        {
            var result = this.GetFirstOrDefault<TEntity>(conditions, orderBy, commandTimeout);
            return result ?? throw new InvalidOperationException($"No entity matching {conditions} was found");
        }

        public TEntity GetSingleOrDefault<TEntity>(string conditions, object parameters = null, int? commandTimeout = null)
        {
            var command = this.commandFactory.MakeGetTopNCommand<TEntity>(2, conditions, null, parameters, commandTimeout);
            return this.DbConnection.Query<TEntity>(command).SingleOrDefault();
        }

        public TEntity GetSingleOrDefault<TEntity>(object conditions, int? commandTimeout = null)
        {
            var command = this.commandFactory.MakeGetTopNCommand<TEntity>(2, conditions, null, commandTimeout);
            return this.DbConnection.Query<TEntity>(command).SingleOrDefault();
        }

        public TEntity GetSingle<TEntity>(
            string conditions,
            object parameters = null,
            int? commandTimeout = null)
            where TEntity : class
        {
            var result = this.GetSingleOrDefault<TEntity>(conditions, parameters, commandTimeout);
            return result ?? throw new InvalidOperationException($"No entity matching {conditions} was found");
        }

        public TEntity GetSingle<TEntity>(object conditions, int? commandTimeout = null)
            where TEntity : class
        {
            var result = this.GetSingleOrDefault<TEntity>(conditions, commandTimeout);
            return result ?? throw new InvalidOperationException($"No entity matching {conditions} was found");
        }

        public IEnumerable<TEntity> GetRange<TEntity>(string conditions, object parameters = null, int? commandTimeout = null)
        {
            var command = this.commandFactory.MakeGetRangeCommand<TEntity>(conditions, parameters, commandTimeout);
            return this.DbConnection.Query<TEntity>(command);
        }

        public IEnumerable<TEntity> GetRange<TEntity>(object conditions, int? commandTimeout = null)
        {
            var command = this.commandFactory.MakeGetRangeCommand<TEntity>(conditions, commandTimeout);
            return this.DbConnection.Query<TEntity>(command);
        }

        public PagedList<TEntity> GetPage<TEntity>(
            IPageBuilder pageBuilder,
            string conditions,
            string orderBy,
            object parameters = null,
            int? commandTimeout = null)
        {
            var totalNumberOfItems = this.Count<TEntity>(conditions, parameters, commandTimeout);
            var page = pageBuilder.GetCurrentPage(totalNumberOfItems);
            if (page.IsEmpty)
            {
                return PagedList<TEntity>.Empty(totalNumberOfItems, page);
            }

            var itemsCommand = this.commandFactory.MakeGetPageCommand<TEntity>(page, conditions, orderBy, parameters, commandTimeout);
            var items = this.DbConnection.Query<TEntity>(itemsCommand);
            return PagedList<TEntity>.Create(totalNumberOfItems, page, items);
        }

        public PagedList<TEntity> GetPage<TEntity>(
            IPageBuilder pageBuilder,
            object conditions,
            string orderBy,
            int? commandTimeout = null)
        {
            var totalNumberOfItems = this.Count<TEntity>(conditions, commandTimeout);
            var page = pageBuilder.GetCurrentPage(totalNumberOfItems);
            if (page.IsEmpty)
            {
                return PagedList<TEntity>.Empty(totalNumberOfItems, page);
            }

            var itemsCommand = this.commandFactory.MakeGetPageCommand<TEntity>(page, conditions, orderBy, commandTimeout);
            var items = this.DbConnection.Query<TEntity>(itemsCommand);
            return PagedList<TEntity>.Create(totalNumberOfItems, page, items);
        }

        public IEnumerable<TEntity> GetAll<TEntity>(int? commandTimeout = null)
        {
            var command = this.commandFactory.MakeGetAllCommand<TEntity>(commandTimeout);
            return this.DbConnection.Query<TEntity>(command);
        }

        public void Insert(object entity, int? commandTimeout = null, bool? verifyAffectedRowCount = null)
        {
            var command = this.commandFactory.MakeInsertCommand(entity, commandTimeout);

            var result = this.ExecuteCommand(command);
            if (this.Config.ShouldVerifyAffectedRowCount(verifyAffectedRowCount))
            {
                result.ExpectingAffectedRowCountToBe(1);
            }
        }

        public TPrimaryKey Insert<TPrimaryKey>(object entity, int? commandTimeout = null)
        {
            var command = this.commandFactory.MakeInsertReturningPrimaryKeyCommand<TPrimaryKey>(entity, commandTimeout);
            return this.DbConnection.ExecuteScalar<TPrimaryKey>(command);
        }

        public SqlCommandResult InsertRange<TEntity>(IEnumerable<TEntity> entities, int? commandTimeout = null)
        {
            var command = this.commandFactory.MakeInsertRangeCommand(entities, commandTimeout);
            return this.ExecuteCommand(command);
        }

        [SuppressMessage("ReSharper", "PossibleMultipleEnumeration", Justification = "Ensure doesn't enumerate")]
        public void InsertRange<TEntity, TPrimaryKey>(IEnumerable<TEntity> entities, Action<TEntity, TPrimaryKey> setPrimaryKey, int? commandTimeout = null)
        {
            Ensure.NotNull(setPrimaryKey, nameof(setPrimaryKey));
            Ensure.NotNull(entities, nameof(entities));

            var sql = this.commandFactory.MakeInsertRangeCommand<TEntity, TPrimaryKey>();

            foreach (var entity in entities)
            {
                var command = new CommandDefinition(sql, entity, this.transaction, commandTimeout);
                var id = this.DbConnection.ExecuteScalar<TPrimaryKey>(command);
                setPrimaryKey(entity, id);
            }
        }

        public void Update<TEntity>(TEntity entity, int? commandTimeout = null, bool? verifyAffectedRowCount = null)
        {
            var command = this.commandFactory.MakeUpdateCommand<TEntity>(entity, commandTimeout);
            var result = this.ExecuteCommand(command);

            if (this.Config.ShouldVerifyAffectedRowCount(verifyAffectedRowCount))
            {
                result.ExpectingAffectedRowCountToBe(1);
            }
        }

        public SqlCommandResult UpdateRange<TEntity>(IEnumerable<TEntity> entities, int? commandTimeout = null)
        {
            var command = this.commandFactory.MakeUpdateRangeCommand(entities, commandTimeout);
            return this.ExecuteCommand(command);
        }

        public void Delete<TEntity>(TEntity entity, int? commandTimeout = null, bool? verifyAffectedRowCount = null)
        {
            var command = this.commandFactory.MakeDeleteCommand<TEntity>(entity, commandTimeout);
            var result = this.ExecuteCommand(command);

            if (this.Config.ShouldVerifyAffectedRowCount(verifyAffectedRowCount))
            {
                result.ExpectingAffectedRowCountToBe(1);
            }
        }

        public void Delete<TEntity>(object id, int? commandTimeout = null, bool? verifyAffectedRowCount = null)
        {
            var command = this.commandFactory.MakeDeleteByPrimaryKeyCommand<TEntity>(id, commandTimeout);
            var result = this.ExecuteCommand(command);

            if (this.Config.ShouldVerifyAffectedRowCount(verifyAffectedRowCount))
            {
                result.ExpectingAffectedRowCountToBe(1);
            }
        }

        public SqlCommandResult DeleteRange<TEntity>(string conditions, object parameters = null, int? commandTimeout = null)
        {
            var command = this.commandFactory.MakeDeleteRangeCommand<TEntity>(conditions, parameters, commandTimeout);
            return this.ExecuteCommand(command);
        }

        public SqlCommandResult DeleteRange<TEntity>(object conditions, int? commandTimeout = null)
        {
            var command = this.commandFactory.MakeDeleteRangeCommand<TEntity>(conditions, commandTimeout);
            return this.ExecuteCommand(command);
        }

        public SqlCommandResult DeleteAll<TEntity>(int? commandTimeout = null)
        {
            var command = this.commandFactory.MakeDeleteAllCommand<TEntity>(commandTimeout);
            return this.ExecuteCommand(command);
        }

        private SqlCommandResult ExecuteCommand(CommandDefinition command)
        {
            return new SqlCommandResult(this.DbConnection.Execute(command));
        }
    }
}