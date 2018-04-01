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