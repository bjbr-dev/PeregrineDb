// <copyright file="ISqlConnection{T}.cs" company="Berkeleybross">
// Copyright (c) Berkeleybross. All rights reserved.
// </copyright>

namespace PeregrineDb
{
    using System.Data;

    public interface ISqlConnection<out TConnection>
        : ISqlConnection
        where TConnection : IDbConnection
    {
        new TConnection DbConnection { get; }
    }
}