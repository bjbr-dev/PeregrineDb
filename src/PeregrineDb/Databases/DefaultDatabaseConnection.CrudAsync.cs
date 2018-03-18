namespace PeregrineDb.Databases
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Dapper;
    using Pagination;
    using PeregrineDb.SqlCommands;
    using PeregrineDb.Utils;

    public partial class DefaultDatabaseConnection
    {
        public Task<int> CountAsync<TEntity>(
            string conditions = null,
            object parameters = null,
            int? commandTimeout = null,
            CancellationToken cancellationToken = default)
        {
            var command = this.commandFactory.MakeCountCommand<TEntity>(conditions, parameters, commandTimeout, cancellationToken);
            return this.connection.ExecuteScalarAsync<int>(command);
        }

        public Task<int> CountAsync<TEntity>(object conditions, int? commandTimeout = null, CancellationToken cancellationToken = default)
        {
            var command = this.commandFactory.MakeCountCommand<TEntity>(conditions, commandTimeout, cancellationToken);
            return this.connection.ExecuteScalarAsync<int>(command);
        }


        public async Task<TEntity> FindAsync<TEntity>(object id, int? commandTimeout = null, CancellationToken cancellationToken = default)
        {
            var command = this.commandFactory.MakeFindCommand<TEntity>(id, commandTimeout, cancellationToken);
            var result = await this.connection.QueryAsync<TEntity>(command).ConfigureAwait(false);
            return result.FirstOrDefault();
        }

        public async Task<TEntity> GetAsync<TEntity>(object id, int? commandTimeout = null, CancellationToken cancellationToken = default)
            where TEntity : class
        {
            var result = await this.FindAsync<TEntity>(id, commandTimeout, cancellationToken).ConfigureAwait(false);
            return result ?? throw new InvalidOperationException($"An entity with id {id} was not found");
        }

        public async Task<TEntity> GetFirstOrDefaultAsync<TEntity>(
            string conditions,
            string orderBy,
            object parameters = null,
            int? commandTimeout = null,
            CancellationToken cancellationToken = default)
        {
            var command = this.commandFactory.MakeGetTopNCommand<TEntity>(1, conditions, orderBy, parameters, commandTimeout, cancellationToken);
            var result = await this.connection.QueryAsync<TEntity>(command).ConfigureAwait(false);
            return result.FirstOrDefault();
        }

        public async Task<TEntity> GetFirstOrDefaultAsync<TEntity>(
            object conditions,
            string orderBy,
            int? commandTimeout = null,
            CancellationToken cancellationToken = default)
        {
            var command = this.commandFactory.MakeGetTopNCommand<TEntity>(1, conditions, orderBy, commandTimeout);
            var result = await this.connection.QueryAsync<TEntity>(command).ConfigureAwait(false);
            return result.FirstOrDefault();
        }

        public async Task<TEntity> GetFirstAsync<TEntity>(
            string conditions,
            string orderBy,
            object parameters = null,
            int? commandTimeout = null,
            CancellationToken cancellationToken = default)
            where TEntity : class
        {
            var result = await this.GetFirstOrDefaultAsync<TEntity>(conditions, orderBy, parameters, commandTimeout, cancellationToken);
            return result ?? throw new InvalidOperationException($"No entity matching {conditions} was found");
        }

        public async Task<TEntity> GetFirstAsync<TEntity>(
            object conditions,
            string orderBy,
            int? commandTimeout = null,
            CancellationToken cancellationToken = default)
            where TEntity : class
        {
            var result = await this.GetFirstOrDefaultAsync<TEntity>(conditions, orderBy, commandTimeout, cancellationToken);
            return result ?? throw new InvalidOperationException($"No entity matching {conditions} was found");
        }

        public async Task<TEntity> GetSingleOrDefaultAsync<TEntity>(
            string conditions,
            object parameters = null,
            int? commandTimeout = null,
            CancellationToken cancellationToken = default)
        {
            var command = this.commandFactory.MakeGetTopNCommand<TEntity>(2, conditions, null, parameters, commandTimeout, cancellationToken);
            var result = await this.connection.QueryAsync<TEntity>(command).ConfigureAwait(false);
            return result.SingleOrDefault();
        }

        public async Task<TEntity> GetSingleOrDefaultAsync<TEntity>(
            object conditions,
            int? commandTimeout = null,
            CancellationToken cancellationToken = default)
        {
            var command = this.commandFactory.MakeGetTopNCommand<TEntity>(2, conditions, null, commandTimeout);
            var result = await this.connection.QueryAsync<TEntity>(command).ConfigureAwait(false);
            return result.SingleOrDefault();
        }

        public async Task<TEntity> GetSingleAsync<TEntity>(
            string conditions,
            object parameters = null,
            int? commandTimeout = null,
            CancellationToken cancellationToken = default)
            where TEntity : class
        {
            var result = await this.GetSingleOrDefaultAsync<TEntity>(conditions, parameters, commandTimeout, cancellationToken);
            return result ?? throw new InvalidOperationException($"No entity matching {conditions} was found");
        }

        public async Task<TEntity> GetSingleAsync<TEntity>(object conditions, int? commandTimeout = null, CancellationToken cancellationToken = default)
            where TEntity : class
        {
            var result = await this.GetSingleOrDefaultAsync<TEntity>(conditions, commandTimeout, cancellationToken);
            return result ?? throw new InvalidOperationException($"No entity matching {conditions} was found");
        }

        public Task<IEnumerable<TEntity>> GetRangeAsync<TEntity>(
            string conditions,
            object parameters = null,
            int? commandTimeout = null,
            CancellationToken cancellationToken = default)
        {
            var command = this.commandFactory.MakeGetRangeCommand<TEntity>(conditions, parameters, commandTimeout, cancellationToken);
            return this.connection.QueryAsync<TEntity>(command);
        }

        public Task<IEnumerable<TEntity>> GetRangeAsync<TEntity>(object conditions, int? commandTimeout = null, CancellationToken cancellationToken = default)
        {
            var command = this.commandFactory.MakeGetRangeCommand<TEntity>(conditions, commandTimeout, cancellationToken);
            return this.connection.QueryAsync<TEntity>(command);
        }

        public async Task<PagedList<TEntity>> GetPageAsync<TEntity>(
            IPageBuilder pageBuilder,
            string conditions,
            string orderBy,
            object parameters = null,
            int? commandTimeout = null,
            CancellationToken cancellationToken = default)
        {
            var totalNumberOfItems = await this.CountAsync<TEntity>(conditions, parameters, commandTimeout, cancellationToken).ConfigureAwait(false);
            var page = pageBuilder.GetCurrentPage(totalNumberOfItems);
            if (page.IsEmpty)
            {
                return PagedList<TEntity>.Empty(totalNumberOfItems, page);
            }

            var itemsCommand = this.commandFactory.MakeGetPageCommand<TEntity>(page, conditions, orderBy, parameters, commandTimeout, cancellationToken);
            var items = await this.connection.QueryAsync<TEntity>(itemsCommand).ConfigureAwait(false);
            return PagedList<TEntity>.Create(totalNumberOfItems, page, items);
        }

        public async Task<PagedList<TEntity>> GetPageAsync<TEntity>(
            IPageBuilder pageBuilder,
            object conditions,
            string orderBy,
            int? commandTimeout = null,
            CancellationToken cancellationToken = default)
        {
            var totalNumberOfItems = await this.CountAsync<TEntity>(conditions, commandTimeout, cancellationToken).ConfigureAwait(false);
            var page = pageBuilder.GetCurrentPage(totalNumberOfItems);
            if (page.IsEmpty)
            {
                return PagedList<TEntity>.Empty(totalNumberOfItems, page);
            }

            var itemsCommand = this.commandFactory.MakeGetPageCommand<TEntity>(page, conditions, orderBy, commandTimeout, cancellationToken);
            var items = await this.connection.QueryAsync<TEntity>(itemsCommand).ConfigureAwait(false);
            return PagedList<TEntity>.Create(totalNumberOfItems, page, items);
        }

        public Task<IEnumerable<TEntity>> GetAllAsync<TEntity>(int? commandTimeout = null)
        {
            var command = this.commandFactory.MakeGetAllCommand<TEntity>(commandTimeout);
            return this.connection.QueryAsync<TEntity>(command);
        }

        public async Task InsertAsync(
            object entity,
            int? commandTimeout = null,
            bool? verifyAffectedRowCount = null,
            CancellationToken cancellationToken = default)
        {
            var command = this.commandFactory.MakeInsertCommand(entity, commandTimeout, cancellationToken);

            var result = await this.ExecuteCommandAsync(command).ConfigureAwait(false);
            if (this.Config.ShouldVerifyAffectedRowCount(verifyAffectedRowCount))
            {
                result.ExpectingAffectedRowCountToBe(1);
            }
        }

        public Task<TPrimaryKey> InsertAsync<TPrimaryKey>(object entity, int? commandTimeout = null, CancellationToken cancellationToken = default)
        {
            var command = this.commandFactory.MakeInsertReturningPrimaryKeyCommand<TPrimaryKey>(entity, commandTimeout, cancellationToken);
            return this.connection.ExecuteScalarAsync<TPrimaryKey>(command);
        }

        public Task<SqlCommandResult> InsertRangeAsync<TEntity>(
            IEnumerable<TEntity> entities,
            int? commandTimeout = null,
            CancellationToken cancellationToken = default)
        {
            var command = this.commandFactory.MakeInsertRangeCommand(entities, commandTimeout, cancellationToken);
            return this.ExecuteCommandAsync(command);
        }

        [SuppressMessage("ReSharper", "PossibleMultipleEnumeration")]
        public async Task InsertRangeAsync<TEntity, TPrimaryKey>(
            IEnumerable<TEntity> entities,
            Action<TEntity, TPrimaryKey> setPrimaryKey,
            int? commandTimeout = null,
            CancellationToken cancellationToken = default)
        {
            Ensure.NotNull(setPrimaryKey, nameof(setPrimaryKey));
            Ensure.NotNull(entities, nameof(entities));

            var sql = this.commandFactory.MakeInsertRangeCommand<TEntity, TPrimaryKey>();

            foreach (var entity in entities)
            {
                var command = new CommandDefinition(sql, entity, this.transaction, commandTimeout, cancellationToken: cancellationToken);
                var id = await this.connection.ExecuteScalarAsync<TPrimaryKey>(command).ConfigureAwait(false);
                setPrimaryKey(entity, id);
            }
        }

        public async Task UpdateAsync<TEntity>(
            TEntity entity,
            int? commandTimeout = null,
            bool? verifyAffectedRowCount = null,
            CancellationToken cancellationToken = default)
        {
            var command = this.commandFactory.MakeUpdateCommand<TEntity>(entity, commandTimeout, cancellationToken);
            var result = await this.ExecuteCommandAsync(command).ConfigureAwait(false);

            if (this.Config.ShouldVerifyAffectedRowCount(verifyAffectedRowCount))
            {
                result.ExpectingAffectedRowCountToBe(1);
            }
        }

        public Task<SqlCommandResult> UpdateRangeAsync<TEntity>(
            IEnumerable<TEntity> entities,
            int? commandTimeout = null,
            CancellationToken cancellationToken = default)
        {
            var command = this.commandFactory.MakeUpdateRangeCommand(entities, commandTimeout, cancellationToken);
            return this.ExecuteCommandAsync(command);
        }

        public async Task DeleteAsync<TEntity>(
            TEntity entity,
            int? commandTimeout = null,
            bool? verifyAffectedRowCount = null,
            CancellationToken cancellationToken = default)
        {
            var command = this.commandFactory.MakeDeleteCommand<TEntity>(entity, commandTimeout, cancellationToken);
            var result = await this.ExecuteCommandAsync(command).ConfigureAwait(false);

            if (this.Config.ShouldVerifyAffectedRowCount(verifyAffectedRowCount))
            {
                result.ExpectingAffectedRowCountToBe(1);
            }
        }

        public async Task DeleteAsync<TEntity>(
            object id,
            int? commandTimeout = null,
            bool? verifyAffectedRowCount = null,
            CancellationToken cancellationToken = default)
        {
            var command = this.commandFactory.MakeDeleteByPrimaryKeyCommand<TEntity>(id, commandTimeout, cancellationToken);
            var result = await this.ExecuteCommandAsync(command).ConfigureAwait(false);

            if (this.Config.ShouldVerifyAffectedRowCount(verifyAffectedRowCount))
            {
                result.ExpectingAffectedRowCountToBe(1);
            }
        }

        public Task<SqlCommandResult> DeleteRangeAsync<TEntity>(
            string conditions,
            object parameters = null,
            int? commandTimeout = null,
            CancellationToken cancellationToken = default)
        {
            var command = this.commandFactory.MakeDeleteRangeCommand<TEntity>(conditions, parameters, commandTimeout, cancellationToken);
            return this.ExecuteCommandAsync(command);
        }

        public Task<SqlCommandResult> DeleteRangeAsync<TEntity>(object conditions, int? commandTimeout = null, CancellationToken cancellationToken = default)
        {
            var command = this.commandFactory.MakeDeleteRangeCommand<TEntity>(conditions, commandTimeout, cancellationToken);
            return this.ExecuteCommandAsync(command);
        }

        public Task<SqlCommandResult> DeleteAllAsync<TEntity>(int? commandTimeout = null, CancellationToken cancellationToken = default)
        {
            var command = this.commandFactory.MakeDeleteAllCommand<TEntity>(commandTimeout, cancellationToken);
            return this.ExecuteCommandAsync(command);
        }

        private async Task<SqlCommandResult> ExecuteCommandAsync(CommandDefinition command)
        {
            return new SqlCommandResult(await this.connection.ExecuteAsync(command).ConfigureAwait(false));
        }
    }
}