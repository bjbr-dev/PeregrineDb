// <copyright file="DefaultDatabase.cs" company="Berkeleybross">
// Copyright (c) Berkeleybross. All rights reserved.
// </copyright>

namespace PeregrineDb.Databases
{
    using System.Data;

    public static class DefaultDatabase
    {
        /// <summary>
        /// Create a new, dynamic instance of <see cref="DefaultDatabase{T}"/>. This method is a light weight wrapper of <see cref="DefaultDatabase{TConnection}(TConnection, PeregrineConfig, bool)"/> for generic inference.
        /// </summary>
        public static IDatabase<TConnection> From<TConnection>(TConnection connection, PeregrineConfig config, bool leaveOpen = false)
            where TConnection : IDbConnection
        {
            return new DefaultDatabase<TConnection>(connection, config, leaveOpen);
        }
    }
}