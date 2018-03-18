namespace PeregrineDb.Databases
{
    using System;
    using System.Data;
    using PeregrineDb.SqlCommands;

    public abstract partial class DefaultDatabaseConnection
        : IDatabaseConnection
    {
        private readonly IDbConnection connection;
        private readonly bool disposeConnection;
        
        private readonly IDbTransaction transaction;
        private readonly CommandFactory commandFactory;
        private bool disposed;

        protected DefaultDatabaseConnection(IDbConnection connection, IDbTransaction transaction, PeregrineConfig config, bool disposeConnection)
        {
            this.connection = connection ?? throw new ArgumentNullException(nameof(connection));
            this.transaction = transaction;
            this.disposeConnection = disposeConnection;
            this.Config = config ?? throw new ArgumentNullException(nameof(config));

            this.commandFactory = new CommandFactory(config, transaction);
        }

        public IDbConnection DbConnection
        {
            get
            {
                this.EnsureNotDisposed();
                return this.connection;
            }
        }

        public PeregrineConfig Config { get; }

        public void Dispose()
        {
            if (this.disposed)
            {
                return;
            }

            try
            {
                this.Dispose(true);
            }
            finally
            {
                if (this.disposeConnection)
                {
                    this.connection.Dispose();
                }

                this.disposed = true;
            }
        }

        protected virtual void Dispose(bool disposing)
        {
        }

        private void EnsureNotDisposed()
        {
            if (this.disposed)
            {
                throw new ObjectDisposedException(this.GetType().FullName);
            }
        }
    }
}