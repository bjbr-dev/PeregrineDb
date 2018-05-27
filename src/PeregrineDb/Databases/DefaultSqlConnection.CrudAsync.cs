namespace PeregrineDb.Databases
{
    using System;
    using System.Collections.Generic;
    using System.Data;
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
            var command = this.Dialect.MakeCountCommand<TEntity>(conditions);
            return this.ExecuteScalarAsync<int>(command.CommandText, command.Parameters, CommandType.Text, commandTimeout, cancellationToken);
        }

        public Task<int> CountAsync<TEntity>(object conditions, int? commandTimeout = null, CancellationToken cancellationToken = default)
        {
            var command = this.Dialect.MakeCountCommand<TEntity>(conditions);
            return this.ExecuteScalarAsync<int>(command.CommandText, command.Parameters, CommandType.Text, commandTimeout, cancellationToken);
        }

        public async Task<bool> ExistsAsync<TEntity>(FormattableString conditions = null, int? commandTimeout = null, CancellationToken cancellationToken = default)
        {
            var count = await this.CountAsync<TEntity>(conditions, commandTimeout, cancellationToken).ConfigureAwait(false);
            return count > 0;
        }

        public async Task<bool> ExistsAsync<TEntity>(object conditions, int? commandTimeout = null, CancellationToken cancellationToken = default)
        {
            var count = await this.CountAsync<TEntity>(conditions, commandTimeout, cancellationToken).ConfigureAwait(false);
            return count > 0;
        }

        public Task<TEntity> FindAsync<TEntity>(object id, int? commandTimeout = null, CancellationToken cancellationToken = default)
        {
            var command = this.Dialect.MakeFindCommand<TEntity>(id);
            return this.QueryFirstOrDefaultAsync<TEntity>(command.CommandText, command.Parameters, CommandType.Text, commandTimeout, cancellationToken);
        }

        public async Task<TEntity> GetAsync<TEntity>(object id, int? commandTimeout = null, CancellationToken cancellationToken = default)
            where TEntity : class
        {
            var result = await this.FindAsync<TEntity>(id, commandTimeout, cancellationToken).ConfigureAwait(false);
            return result ?? throw new InvalidOperationException($"An entity with id {id} was not found");
        }

        public Task<TEntity> FindFirstAsync<TEntity>(FormattableString conditions, string orderBy, int? commandTimeout = null, CancellationToken cancellationToken = default)
        {
            var command = this.Dialect.MakeGetFirstNCommand<TEntity>(1, conditions, orderBy);
            return this.QueryFirstOrDefaultAsync<TEntity>(command.CommandText, command.Parameters, CommandType.Text, commandTimeout, cancellationToken);
        }

        public Task<TEntity> FindFirstAsync<TEntity>(object conditions, string orderBy, int? commandTimeout = null, CancellationToken cancellationToken = default)
        {
            var command = this.Dialect.MakeGetFirstNCommand<TEntity>(1, conditions, orderBy);
            return this.QueryFirstOrDefaultAsync<TEntity>(command.CommandText, command.Parameters, CommandType.Text, commandTimeout, cancellationToken);
        }

        public Task<TEntity> GetFirstAsync<TEntity>(FormattableString conditions, string orderBy, int? commandTimeout = null, CancellationToken cancellationToken = default)
            where TEntity : class
        {
            var command = this.Dialect.MakeGetFirstNCommand<TEntity>(1, conditions, orderBy);
            return this.QueryFirstAsync<TEntity>(command.CommandText, command.Parameters, CommandType.Text, commandTimeout, cancellationToken);
        }

        public Task<TEntity> GetFirstAsync<TEntity>(object conditions, string orderBy, int? commandTimeout = null, CancellationToken cancellationToken = default)
            where TEntity : class
        {
            var command = this.Dialect.MakeGetFirstNCommand<TEntity>(1, conditions, orderBy);
            return this.QueryFirstAsync<TEntity>(command.CommandText, command.Parameters, CommandType.Text, commandTimeout, cancellationToken);
        }

        public Task<TEntity> FindSingleAsync<TEntity>(FormattableString conditions, int? commandTimeout = null, CancellationToken cancellationToken = default)
        {
            var command = this.Dialect.MakeGetFirstNCommand<TEntity>(2, conditions, null);
            return this.QuerySingleOrDefaultAsync<TEntity>(command.CommandText, command.Parameters, CommandType.Text, commandTimeout, cancellationToken);
        }

        public Task<TEntity> FindSingleAsync<TEntity>(object conditions, int? commandTimeout = null, CancellationToken cancellationToken = default)
        {
            var command = this.Dialect.MakeGetFirstNCommand<TEntity>(2, conditions, null);
            return this.QuerySingleOrDefaultAsync<TEntity>(command.CommandText, command.Parameters, CommandType.Text, commandTimeout, cancellationToken);
        }

        public async Task<TEntity> GetSingleAsync<TEntity>(FormattableString conditions, int? commandTimeout = null, CancellationToken cancellationToken = default)
            where TEntity : class
        {
            var result = await this.FindSingleAsync<TEntity>(conditions, commandTimeout, cancellationToken);
            return result ?? throw new InvalidOperationException("No entity matching given conditions was found");
        }

        public async Task<TEntity> GetSingleAsync<TEntity>(object conditions, int? commandTimeout = null, CancellationToken cancellationToken = default)
            where TEntity : class
        {
            var result = await this.FindSingleAsync<TEntity>(conditions, commandTimeout, cancellationToken);
            return result ?? throw new InvalidOperationException("No entity matching given conditions was found");
        }

        public Task<IReadOnlyList<TEntity>> GetRangeAsync<TEntity>(FormattableString conditions, int? commandTimeout = null, CancellationToken cancellationToken = default)
        {
            var command = this.Dialect.MakeGetRangeCommand<TEntity>(conditions);
            return this.QueryAsync<TEntity>(command.CommandText, command.Parameters, CommandType.Text, commandTimeout, cancellationToken);
        }

        public Task<IReadOnlyList<TEntity>> GetRangeAsync<TEntity>(object conditions, int? commandTimeout = null, CancellationToken cancellationToken = default)
        {
            var command = this.Dialect.MakeGetRangeCommand<TEntity>(conditions);
            return this.QueryAsync<TEntity>(command.CommandText, command.Parameters, CommandType.Text, commandTimeout, cancellationToken);
        }

        public Task<IReadOnlyList<TEntity>> GetTopAsync<TEntity>(int count, string orderBy, int? commandTimeout = null, CancellationToken cancellationToken = default)
        {
            var command = this.Dialect.MakeGetFirstNCommand<TEntity>(count, orderBy);
            return this.QueryAsync<TEntity>(command.CommandText, command.Parameters, CommandType.Text, commandTimeout, cancellationToken);
        }

        public Task<IReadOnlyList<TEntity>> GetTopAsync<TEntity>(int count, FormattableString conditions, string orderBy, int? commandTimeout = null, CancellationToken cancellationToken = default)
        {
            var command = this.Dialect.MakeGetFirstNCommand<TEntity>(count, conditions, orderBy);
            return this.QueryAsync<TEntity>(command.CommandText, command.Parameters, CommandType.Text, commandTimeout, cancellationToken);
        }

        public Task<IReadOnlyList<TEntity>> GetTopAsync<TEntity>(int count, object conditions, string orderBy, int? commandTimeout = null, CancellationToken cancellationToken = default)
        {
            var command = this.Dialect.MakeGetFirstNCommand<TEntity>(count, conditions, orderBy);
            return this.QueryAsync<TEntity>(command.CommandText, command.Parameters, CommandType.Text, commandTimeout, cancellationToken);
        }

        public async Task<PagedList<TEntity>> GetPageAsync<TEntity>(IPageBuilder pageBuilder, FormattableString conditions, string orderBy, int? commandTimeout = null, CancellationToken cancellationToken = default)
        {
            var totalNumberOfItems = await this.CountAsync<TEntity>(conditions, commandTimeout, cancellationToken).ConfigureAwait(false);
            var page = pageBuilder.GetCurrentPage(totalNumberOfItems);
            if (page.IsEmpty)
            {
                return PagedList<TEntity>.Empty(totalNumberOfItems, page);
            }

            var itemsCommand = this.Dialect.MakeGetPageCommand<TEntity>(page, conditions, orderBy);
            var items = await this.QueryAsync<TEntity>(itemsCommand.CommandText, itemsCommand.Parameters, CommandType.Text, commandTimeout, cancellationToken).ConfigureAwait(false);
            return PagedList<TEntity>.Create(totalNumberOfItems, page, items);
        }

        public async Task<PagedList<TEntity>> GetPageAsync<TEntity>(IPageBuilder pageBuilder, object conditions, string orderBy, int? commandTimeout = null, CancellationToken cancellationToken = default)
        {
            var totalNumberOfItems = await this.CountAsync<TEntity>(conditions, commandTimeout, cancellationToken).ConfigureAwait(false);
            var page = pageBuilder.GetCurrentPage(totalNumberOfItems);
            if (page.IsEmpty)
            {
                return PagedList<TEntity>.Empty(totalNumberOfItems, page);
            }

            var itemsCommand = this.Dialect.MakeGetPageCommand<TEntity>(page, conditions, orderBy);
            var items = await this.QueryAsync<TEntity>(itemsCommand.CommandText, itemsCommand.Parameters, CommandType.Text, commandTimeout, cancellationToken).ConfigureAwait(false);
            return PagedList<TEntity>.Create(totalNumberOfItems, page, items);
        }

        public Task<IReadOnlyList<TEntity>> GetAllAsync<TEntity>(int? commandTimeout = null, CancellationToken cancellationToken = default)
        {
            var command = this.Dialect.MakeGetRangeCommand<TEntity>(null);
            return this.QueryAsync<TEntity>(command.CommandText, command.Parameters, CommandType.Text, commandTimeout, cancellationToken);
        }

        public async Task InsertAsync(object entity, int? commandTimeout = null, bool? verifyAffectedRowCount = null, CancellationToken cancellationToken = default)
        {
            var command = this.Dialect.MakeInsertCommand(entity);

            var result = await this.ExecuteAsync(command.CommandText, command.Parameters, CommandType.Text, commandTimeout, cancellationToken).ConfigureAwait(false);
            if (this.Config.ShouldVerifyAffectedRowCount(verifyAffectedRowCount))
            {
                result.ExpectingAffectedRowCountToBe(1);
            }
        }

        public Task<TPrimaryKey> InsertAsync<TPrimaryKey>(object entity, int? commandTimeout = null, CancellationToken cancellationToken = default)
        {
            var command = this.Dialect.MakeInsertReturningPrimaryKeyCommand<TPrimaryKey>(entity);
            return this.ExecuteScalarAsync<TPrimaryKey>(command.CommandText, command.Parameters, CommandType.Text, commandTimeout, cancellationToken);
        }

        public Task<CommandResult> InsertRangeAsync<TEntity>(IEnumerable<TEntity> entities, int? commandTimeout = null, CancellationToken cancellationToken = default)
        {
            var (sql, parameters) = this.Dialect.MakeInsertRangeCommand(entities);
            return this.ExecuteMultipleAsync(sql, parameters, CommandType.Text, commandTimeout, cancellationToken);
        }

        [SuppressMessage("ReSharper", "PossibleMultipleEnumeration")]
        public async Task InsertRangeAsync<TEntity, TPrimaryKey>(IEnumerable<TEntity> entities, Action<TEntity, TPrimaryKey> setPrimaryKey, int? commandTimeout = null, CancellationToken cancellationToken = default)
        {
            Ensure.NotNull(setPrimaryKey, nameof(setPrimaryKey));
            Ensure.NotNull(entities, nameof(entities));

            foreach (var entity in entities)
            {
                var command = this.Dialect.MakeInsertReturningPrimaryKeyCommand<TPrimaryKey>(entity);
                var id = await this.ExecuteScalarAsync<TPrimaryKey>(command.CommandText, command.Parameters, CommandType.Text, commandTimeout, cancellationToken).ConfigureAwait(false);
                setPrimaryKey(entity, id);
            }
        }

        public async Task UpdateAsync<TEntity>(TEntity entity, int? commandTimeout = null, bool? verifyAffectedRowCount = null, CancellationToken cancellationToken = default)
            where TEntity : class
        {
            var command = this.Dialect.MakeUpdateCommand(entity);
            var result = await this.ExecuteAsync(command.CommandText, command.Parameters, CommandType.Text, commandTimeout, cancellationToken)
                                   .ConfigureAwait(false);

            if (this.Config.ShouldVerifyAffectedRowCount(verifyAffectedRowCount))
            {
                result.ExpectingAffectedRowCountToBe(1);
            }
        }

        public Task<CommandResult> UpdateRangeAsync<TEntity>(IEnumerable<TEntity> entities, int? commandTimeout = null, CancellationToken cancellationToken = default)
            where TEntity : class
        {
            var (sql, parameters) = this.Dialect.MakeUpdateRangeCommand(entities);
            return this.ExecuteMultipleAsync(sql, parameters, CommandType.Text, commandTimeout, cancellationToken);
        }

        public async Task DeleteAsync<TEntity>(TEntity entity, int? commandTimeout = null, bool? verifyAffectedRowCount = null, CancellationToken cancellationToken = default)
            where TEntity : class
        {
            var command = this.Dialect.MakeDeleteCommand(entity);
            var result = await this.ExecuteAsync(command.CommandText, command.Parameters, CommandType.Text, commandTimeout, cancellationToken)
                                   .ConfigureAwait(false);

            if (this.Config.ShouldVerifyAffectedRowCount(verifyAffectedRowCount))
            {
                result.ExpectingAffectedRowCountToBe(1);
            }
        }

        public async Task DeleteAsync<TEntity>(object id, int? commandTimeout = null, bool? verifyAffectedRowCount = null, CancellationToken cancellationToken = default)
        {
            var command = this.Dialect.MakeDeleteByPrimaryKeyCommand<TEntity>(id);
            var result = await this.ExecuteAsync(command.CommandText, command.Parameters, CommandType.Text, commandTimeout, cancellationToken)
                                   .ConfigureAwait(false);

            if (this.Config.ShouldVerifyAffectedRowCount(verifyAffectedRowCount))
            {
                result.ExpectingAffectedRowCountToBe(1);
            }
        }

        public Task<CommandResult> DeleteRangeAsync<TEntity>(FormattableString conditions, int? commandTimeout = null, CancellationToken cancellationToken = default)
        {
            var command = this.Dialect.MakeDeleteRangeCommand<TEntity>(conditions);
            return this.ExecuteAsync(command.CommandText, command.Parameters, CommandType.Text, commandTimeout, cancellationToken);
        }

        public Task<CommandResult> DeleteRangeAsync<TEntity>(object conditions, int? commandTimeout = null, CancellationToken cancellationToken = default)
        {
            var command = this.Dialect.MakeDeleteRangeCommand<TEntity>(conditions);
            return this.ExecuteAsync(command.CommandText, command.Parameters, CommandType.Text, commandTimeout, cancellationToken);
        }

        public Task<CommandResult> DeleteAllAsync<TEntity>(int? commandTimeout = null, CancellationToken cancellationToken = default)
        {
            var command = this.Dialect.MakeDeleteAllCommand<TEntity>();
            return this.ExecuteAsync(command.CommandText, command.Parameters, CommandType.Text, commandTimeout, cancellationToken);
        }
    }
}