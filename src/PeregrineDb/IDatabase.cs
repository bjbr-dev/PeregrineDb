namespace PeregrineDb
{
    using System.Data;

    public interface IDatabase
        : IDatabaseConnection
    {
        /// <summary>
        /// Starts a new <see cref="IDatabaseUnitOfWork"/> with the default <see cref="IsolationLevel"/>.
        /// </summary>
        /// <param name="leaveOpen">
        /// When true (default), the created <see cref="IDatabaseUnitOfWork"/> will leave the underlying connection open when it is disposed.
        /// When false, the created <see cref="IDatabaseUnitOfWork"/> will take ownership of the connection and dispose it when it itself is disposed.
        /// This allows you to let the the current <see cref="IDatabase"/> instance fall out of scope safely.
        /// </param>
        /// <returns></returns>
        IDatabaseUnitOfWork StartUnitOfWork(bool leaveOpen = true);

        /// <summary>
        /// Starts a new <see cref="IDatabaseUnitOfWork"/> with the specified <paramref name="isolationLevel"/>.
        /// </summary>
        /// <param name="leaveOpen">
        /// When true (default), the created <see cref="IDatabaseUnitOfWork"/> will leave the underlying connection open when it is disposed.
        /// When false, the created <see cref="IDatabaseUnitOfWork"/> will take ownership of the connection and dispose it when it itself is disposed.
        /// This allows you to let the the current <see cref="IDatabase"/> instance fall out of scope safely.
        /// </param>
        /// <returns></returns>
        IDatabaseUnitOfWork StartUnitOfWork(IsolationLevel isolationLevel, bool leaveOpen = true);
    }
}