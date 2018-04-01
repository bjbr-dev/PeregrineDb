namespace PeregrineDb
{
    using System.Data;

    public interface IDatabase<out TConnection>
        : IDatabase, ISqlConnection<TConnection>
        where TConnection : IDbConnection
    {
        /// <summary>
        /// Starts a new <see cref="ISqlUnitOfWork"/> with the default <see cref="IsolationLevel"/>.
        /// </summary>
        /// <param name="leaveOpen">
        /// When true (default), the created <see cref="ISqlUnitOfWork"/> will leave the underlying connection open when it is disposed.
        /// When false, the created <see cref="ISqlUnitOfWork"/> will take ownership of the connection and dispose it when it itself is disposed.
        /// This allows you to let the the current <see cref="IDatabase"/> instance fall out of scope safely.
        /// </param>
        /// <returns></returns>
        ISqlUnitOfWork<TConnection, TTransaction> StartUnitOfWork<TTransaction>(bool leaveOpen = true)
            where TTransaction : class, IDbTransaction;

        /// <summary>
        /// Starts a new <see cref="ISqlUnitOfWork"/> with the specified <paramref name="isolationLevel"/>.
        /// </summary>
        /// <param name="isolationLevel"></param>
        /// <param name="leaveOpen">
        /// When true (default), the created <see cref="ISqlUnitOfWork"/> will leave the underlying connection open when it is disposed.
        /// When false, the created <see cref="ISqlUnitOfWork"/> will take ownership of the connection and dispose it when it itself is disposed.
        /// This allows you to let the the current <see cref="IDatabase"/> instance fall out of scope safely.
        /// </param>
        ISqlUnitOfWork<TConnection, TTransaction> StartUnitOfWork<TTransaction>(IsolationLevel isolationLevel, bool leaveOpen = true)
            where TTransaction : class, IDbTransaction;
    }
}