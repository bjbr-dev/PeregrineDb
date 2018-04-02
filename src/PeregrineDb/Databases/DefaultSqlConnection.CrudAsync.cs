namespace PeregrineDb.Databases
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;
    using System.Threading;
    using System.Threading.Tasks;
    using Pagination;
    using PeregrineDb.SqlCommands;
    using PeregrineDb.Utils;

    public partial class DefaultSqlConnection
    {
        public Task<int> CountAsync<TEntity>(FormattableString conditions = null, int? commandTimeout = null, CancellationToken cancellationToken = default)
        {
            return this.ExecuteScalarAsync<int>(this.Dialect.MakeCountCommand<TEntity>(conditions), commandTimeout, cancellationToken);
        }

        public Task<int> CountAsync<TEntity>(object conditions, int? commandTimeout = null, CancellationToken cancellationToken = default)
        {
            return this.ExecuteScalarAsync<int>(this.Dialect.MakeCountCommand<TEntity>(conditions), commandTimeout, cancellationToken);
        }


        public Task<TEntity> FindAsync<TEntity>(object id, int? commandTimeout = null, CancellationToken cancellationToken = default)
        {
            return this.QueryFirstOrDefaultAsync<TEntity>(this.Dialect.MakeFindCommand<TEntity>(id), commandTimeout, cancellationToken);
        }

        public async Task<TEntity> GetAsync<TEntity>(object id, int? commandTimeout = null, CancellationToken cancellationToken = default)
            where TEntity : class
        {
            var result = await this.FindAsync<TEntity>(id, commandTimeout, cancellationToken).ConfigureAwait(false);
            return result ?? throw new InvalidOperationException($"An entity with id {id} was not found");
        }

        public Task<TEntity> GetFirstOrDefaultAsync<TEntity>(
            FormattableString conditions,
            string orderBy,
            int? commandTimeout = null,
            CancellationToken cancellationToken = default)
        {
            var sql = this.Dialect.MakeGetFirstNCommand<TEntity>(1, conditions, orderBy);
            return this.QueryFirstOrDefaultAsync<TEntity>(sql, commandTimeout, cancellationToken);
        }

        public Task<TEntity> GetFirstOrDefaultAsync<TEntity>(
            object conditions,
            string orderBy,
            int? commandTimeout = null,
            CancellationToken cancellationToken = default)
        {
            var command = this.Dialect.MakeGetFirstNCommand<TEntity>(1, conditions, orderBy);
            return this.QueryFirstOrDefaultAsync<TEntity>(command, commandTimeout, cancellationToken);
        }

        public Task<TEntity> GetFirstAsync<TEntity>(
            FormattableString conditions,
            string orderBy,
            int? commandTimeout = null,
            CancellationToken cancellationToken = default)
            where TEntity : class
        {
            var command = this.Dialect.MakeGetFirstNCommand<TEntity>(1, conditions, orderBy);
            return this.QueryFirstAsync<TEntity>(command, commandTimeout, cancellationToken);
        }

        public Task<TEntity> GetFirstAsync<TEntity>(
            object conditions,
            string orderBy,
            int? commandTimeout = null,
            CancellationToken cancellationToken = default)
            where TEntity : class
        {
            var command = this.Dialect.MakeGetFirstNCommand<TEntity>(1, conditions, orderBy);
            return this.QueryFirstAsync<TEntity>(command, commandTimeout, cancellationToken);
        }

        public Task<TEntity> GetSingleOrDefaultAsync<TEntity>(
            FormattableString conditions,
            int? commandTimeout = null,
            CancellationToken cancellationToken = default)
        {
            var command = this.Dialect.MakeGetFirstNCommand<TEntity>(2, conditions, null);
            return this.QuerySingleOrDefaultAsync<TEntity>(command, commandTimeout, cancellationToken);
        }

        public Task<TEntity> GetSingleOrDefaultAsync<TEntity>(
            object conditions,
            int? commandTimeout = null,
            CancellationToken cancellationToken = default)
        {
            var command = this.Dialect.MakeGetFirstNCommand<TEntity>(2, conditions, null);
            return this.QuerySingleOrDefaultAsync<TEntity>(command, commandTimeout, cancellationToken);
        }

        public async Task<TEntity> GetSingleAsync<TEntity>(
            FormattableString conditions,
            int? commandTimeout = null,
            CancellationToken cancellationToken = default)
            where TEntity : class
        {
            var result = await this.GetSingleOrDefaultAsync<TEntity>(conditions, commandTimeout, cancellationToken);
            return result ?? throw new InvalidOperationException($"No entity matching {conditions} was found");
        }

        public async Task<TEntity> GetSingleAsync<TEntity>(object conditions, int? commandTimeout = null, CancellationToken cancellationToken = default)
            where TEntity : class
        {
            var result = await this.GetSingleOrDefaultAsync<TEntity>(conditions, commandTimeout, cancellationToken);
            return result ?? throw new InvalidOperationException($"No entity matching {conditions} was found");
        }

        public Task<IEnumerable<TEntity>> GetRangeAsync<TEntity>(
            FormattableString conditions,
            int? commandTimeout = null,
            CancellationToken cancellationToken = default)
        {
            var command = this.Dialect.MakeGetRangeCommand<TEntity>(conditions);
            return this.QueryAsync<TEntity>(command, commandTimeout, cancellationToken);
        }

        public Task<IEnumerable<TEntity>> GetRangeAsync<TEntity>(object conditions, int? commandTimeout = null, CancellationToken cancellationToken = default)
        {
            var command = this.Dialect.MakeGetRangeCommand<TEntity>(conditions);
            return this.QueryAsync<TEntity>(command, commandTimeout, cancellationToken);
        }

        public async Task<PagedList<TEntity>> GetPageAsync<TEntity>(
            IPageBuilder pageBuilder,
            FormattableString conditions,
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

            var itemsCommand = this.Dialect.MakeGetPageCommand<TEntity>(page, conditions, orderBy);
            var items = await this.QueryAsync<TEntity>(itemsCommand, commandTimeout, cancellationToken).ConfigureAwait(false);
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

            var itemsCommand = this.Dialect.MakeGetPageCommand<TEntity>(page, conditions, orderBy);
            var items = await this.QueryAsync<TEntity>(itemsCommand, commandTimeout, cancellationToken).ConfigureAwait(false);
            return PagedList<TEntity>.Create(totalNumberOfItems, page, items);
        }

        public Task<IEnumerable<TEntity>> GetAllAsync<TEntity>(int? commandTimeout = null, CancellationToken cancellationToken = default)
        {
            return this.QueryAsync<TEntity>(this.Dialect.MakeGetRangeCommand<TEntity>(null), commandTimeout, cancellationToken);
        }

        public async Task InsertAsync(
            object entity,
            int? commandTimeout = null,
            bool? verifyAffectedRowCount = null,
            CancellationToken cancellationToken = default)
        {
            var command = this.Dialect.MakeInsertCommand(entity);

            var result = await this.ExecuteAsync(command, commandTimeout, cancellationToken).ConfigureAwait(false);
            if (this.Config.ShouldVerifyAffectedRowCount(verifyAffectedRowCount))
            {
                result.ExpectingAffectedRowCountToBe(1);
            }
        }

        public Task<TPrimaryKey> InsertAsync<TPrimaryKey>(object entity, int? commandTimeout = null, CancellationToken cancellationToken = default)
        {
            var command = this.Dialect.MakeInsertReturningPrimaryKeyCommand<TPrimaryKey>(entity);
            return this.ExecuteScalarAsync<TPrimaryKey>(command, commandTimeout, cancellationToken);
        }

        public async Task<CommandResult> InsertRangeAsync<TEntity>(
            IEnumerable<TEntity> entities,
            int? commandTimeout = null,
            CancellationToken cancellationToken = default)
        {
            var num = 0;
            foreach (var entity in entities)
            {
                await this.InsertAsync(entity, commandTimeout, false, cancellationToken).ConfigureAwait(false);
                num++;
            }

            return new CommandResult(num);
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

            foreach (var entity in entities)
            {
                var sql = this.Dialect.MakeInsertReturningPrimaryKeyCommand<TPrimaryKey>(entity);
                var id = await this.ExecuteScalarAsync<TPrimaryKey>(sql, commandTimeout, cancellationToken).ConfigureAwait(false);
                setPrimaryKey(entity, id);
            }
        }

        public async Task UpdateAsync<TEntity>(
            TEntity entity,
            int? commandTimeout = null,
            bool? verifyAffectedRowCount = null,
            CancellationToken cancellationToken = default)
            where TEntity : class
        {
            var result = await this.ExecuteAsync(this.Dialect.MakeUpdateCommand(entity), commandTimeout, cancellationToken)
                                   .ConfigureAwait(false);

            if (this.Config.ShouldVerifyAffectedRowCount(verifyAffectedRowCount))
            {
                result.ExpectingAffectedRowCountToBe(1);
            }
        }

        public async Task<CommandResult> UpdateRangeAsync<TEntity>(
            IEnumerable<TEntity> entities,
            int? commandTimeout = null,
            CancellationToken cancellationToken = default)
            where TEntity : class
        {
            var num = 0;
            foreach (var entity in entities)
            {
                await this.ExecuteAsync(this.Dialect.MakeUpdateCommand(entity), commandTimeout, cancellationToken).ConfigureAwait(false);
                num++;
            }

            return new CommandResult(num);
        }

        public async Task DeleteAsync<TEntity>(
            TEntity entity,
            int? commandTimeout = null,
            bool? verifyAffectedRowCount = null,
            CancellationToken cancellationToken = default) 
            where TEntity : class
        {
            var result = await this.ExecuteAsync(this.Dialect.MakeDeleteCommand(entity), commandTimeout, cancellationToken)
                                   .ConfigureAwait(false);

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
            var result = await this.ExecuteAsync(this.Dialect.MakeDeleteByPrimaryKeyCommand<TEntity>(id), commandTimeout, cancellationToken)
                                   .ConfigureAwait(false);

            if (this.Config.ShouldVerifyAffectedRowCount(verifyAffectedRowCount))
            {
                result.ExpectingAffectedRowCountToBe(1);
            }
        }

        public Task<CommandResult> DeleteRangeAsync<TEntity>(
            FormattableString conditions,
            int? commandTimeout = null,
            CancellationToken cancellationToken = default)
        {
            return this.ExecuteAsync(this.Dialect.MakeDeleteRangeCommand<TEntity>(conditions), commandTimeout, cancellationToken);
        }

        public Task<CommandResult> DeleteRangeAsync<TEntity>(object conditions, int? commandTimeout = null, CancellationToken cancellationToken = default)
        {
            return this.ExecuteAsync(this.Dialect.MakeDeleteRangeCommand<TEntity>(conditions), commandTimeout, cancellationToken);
        }

        public Task<CommandResult> DeleteAllAsync<TEntity>(int? commandTimeout = null, CancellationToken cancellationToken = default)
        {
            return this.ExecuteAsync(this.Dialect.MakeDeleteAllCommand<TEntity>(), commandTimeout, cancellationToken);
        }
    }
}