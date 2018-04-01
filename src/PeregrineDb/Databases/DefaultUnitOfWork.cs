namespace PeregrineDb.Databases
{
    using System.Data;

    public static class DefaultUnitOfWork
    {
        /// <summary>
        /// Create a new, dynamic instance of <see cref="DefaultUnitOfWork{TConnection,TTransaction}"/>. This method is a light weight wrapper for generic inference.
        /// </summary>
        public static DefaultUnitOfWork<TConnection, TTransaction> From<TConnection, TTransaction>(
            TConnection connection,
            TTransaction transaction,
            PeregrineConfig config,
            bool leaveOpen = false)
            where TConnection : IDbConnection
            where TTransaction : class, IDbTransaction
        {
            return new DefaultUnitOfWork<TConnection, TTransaction>(connection, transaction, config, leaveOpen);
        }
    }
}