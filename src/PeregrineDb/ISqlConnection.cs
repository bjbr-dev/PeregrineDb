// <copyright file="ISqlConnection.cs" company="Berkeleybross">
// Copyright (c) Berkeleybross. All rights reserved.
// </copyright>

namespace PeregrineDb
{
    using System;
    using System.Data;

    public partial interface ISqlConnection
        : IDisposable
    {
        IDbConnection DbConnection { get; }

        PeregrineConfig Config { get; }
    }
}