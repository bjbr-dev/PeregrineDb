namespace PeregrineDb
{
    using System.Data;

    public interface IDatabase<out TConnection>
        : ISqlConnection<TConnection>
        where TConnection : IDbConnection
    {
        /// <summary>
        /// Starts a new <see cref="ISqlUnitOfWork{TConnection, TTransaction}"/> with the default <see cref="IsolationLevel"/>.
        /// </summary>
        /// <param name="leaveOpen">
        /// When true (default), the created <see cref="ISqlUnitOfWork{TConnection, TTransaction}"/> will leave the underlying connection open when it is disposed.
        /// When false, the created <see cref="ISqlUnitOfWork{TConnection, TTransaction}"/> will take ownership of the connection and dispose it when it itself is disposed.
        /// This allows you to let the the current <see cref="IDatabase{TConnection}"/> instance fall out of scope safely.
        /// </param>
        ISqlUnitOfWork<TConnection, IDbTransaction> StartUnitOfWork(bool leaveOpen = true);

        /// <summary>
        /// Starts a new <see cref="ISqlUnitOfWork{TConnection, TTransaction}"/> with the specified <paramref name="isolationLevel"/>.
        /// </summary>
        /// <param name="isolationLevel"></param>
        /// <param name="leaveOpen">
        /// When true (default), the created <see cref="ISqlUnitOfWork{TConnection, TTransaction}"/> will leave the underlying connection open when it is disposed.
        /// When false, the created <see cref="ISqlUnitOfWork{TConnection, TTransaction}"/> will take ownership of the connection and dispose it when it itself is disposed.
        /// This allows you to let the the current <see cref="IDatabase{TConnection}"/> instance fall out of scope safely.
        /// </param>
        ISqlUnitOfWork<TConnection, IDbTransaction> StartUnitOfWork(IsolationLevel isolationLevel, bool leaveOpen = true);

        /// <summary>
        /// Starts a new <see cref="ISqlUnitOfWork{TConnection, TTransaction}"/> with the default <see cref="IsolationLevel"/>.
        /// </summary>
        /// <param name="leaveOpen">
        /// When true (default), the created <see cref="ISqlUnitOfWork{TConnection, TTransaction}"/> will leave the underlying connection open when it is disposed.
        /// When false, the created <see cref="ISqlUnitOfWork{TConnection, TTransaction}"/> will take ownership of the connection and dispose it when it itself is disposed.
        /// This allows you to let the the current <see cref="IDatabase{TConnection}"/> instance fall out of scope safely.
        /// </param>
        ISqlUnitOfWork<TConnection, TTransaction> StartUnitOfWork<TTransaction>(bool leaveOpen = true)
            where TTransaction : class, IDbTransaction;

        /// <summary>
        /// Starts a new <see cref="ISqlUnitOfWork{TConnection, TTransaction}"/> with the specified <paramref name="isolationLevel"/>.
        /// </summary>
        /// <param name="isolationLevel"></param>
        /// <param name="leaveOpen">
        /// When true (default), the created <see cref="ISqlUnitOfWork{TConnection, TTransaction}"/> will leave the underlying connection open when it is disposed.
        /// When false, the created <see cref="ISqlUnitOfWork{TConnection, TTransaction}"/> will take ownership of the connection and dispose it when it itself is disposed.
        /// This allows you to let the the current <see cref="IDatabase{TConnection}"/> instance fall out of scope safely.
        /// </param>
        ISqlUnitOfWork<TConnection, TTransaction> StartUnitOfWork<TTransaction>(IsolationLevel isolationLevel, bool leaveOpen = true)
            where TTransaction : class, IDbTransaction;
    }
}