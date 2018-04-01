namespace PeregrineDb.Databases
{
    using System;
    using System.Data;
    using PeregrineDb.Dialects;
    using PeregrineDb.SqlCommands;

    public abstract partial class DefaultSqlConnection
        : ISqlConnection
    {
        private readonly IDbConnection connection;
        private readonly bool leaveOpen;
        
        private readonly IDbTransaction transaction;
        private readonly CommandFactory commandFactory;
        private bool disposed;

        protected DefaultSqlConnection(IDbConnection connection, IDbTransaction transaction, PeregrineConfig config, bool leaveOpen)
        {
            this.connection = connection ?? throw new ArgumentNullException(nameof(connection));
            this.transaction = transaction;
            this.leaveOpen = leaveOpen;
            this.Config = config ?? throw new ArgumentNullException(nameof(config));

            this.commandFactory = new CommandFactory(config);
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

        public IDialect Dialect => this.Config.Dialect;

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
                if (!this.leaveOpen)
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