namespace PeregrineDb.Databases
{
    using System.Data;

    public static class DefaultDatabase
    {
        /// <summary>
        /// Create a new, dynamic instance of <see cref="DefaultDatabase{T}"/>. This method is a light weight wrapper for generic inference.
        /// </summary>
        public static DefaultDatabase<TConnection> From<TConnection>(TConnection connection, PeregrineConfig config, bool leaveOpen = false)
            where TConnection : IDbConnection
        {
            return new DefaultDatabase<TConnection>(connection, config, leaveOpen);
        }
    }
}