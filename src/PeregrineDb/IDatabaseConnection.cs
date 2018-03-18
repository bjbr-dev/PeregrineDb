namespace PeregrineDb
{
    using System;
    using System.Data;

    public partial interface IDatabaseConnection
        : IDisposable
    {
        IDbConnection DbConnection { get; }

        PeregrineConfig Config { get; }
    }
}