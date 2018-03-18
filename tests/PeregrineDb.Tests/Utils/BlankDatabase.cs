namespace PeregrineDb.Tests.Utils
{
    using System;
    using System.Data;
    using PeregrineDb.Dialects;

    public class BlankDatabase
        : IDisposable
    {
        private readonly Action dropDatabase;
        private bool disposed;

        public BlankDatabase(IDialect dialect, IDbConnection connection, Action dropDatabase)
        {
            this.dropDatabase = dropDatabase;
        }

        public IDatabase Database { get; set; }

        public void Dispose()
        {
            if (this.disposed)
            {
                return;
            }

            this.dropDatabase();
            this.disposed = true;
        }
    }
}