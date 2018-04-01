namespace PeregrineDb.Tests.Utils
{
    using System.Data;
    using PeregrineDb.Tests.Utils.Pooling;

    public class PooledConnection<T>
        : IDbConnection
        where T : class, IDbConnection
    {
        private readonly PooledInstance<string> pooledInstance;
        private readonly T connection;

        public PooledConnection(PooledInstance<string> pooledInstance, T connection)
        {
            this.pooledInstance = pooledInstance;
            this.connection = connection;
        }

        public void Dispose()
        {
            try
            {
                this.connection.Dispose();
            }
            finally
            {
                this.pooledInstance.Dispose();
            }
        }

        public IDbTransaction BeginTransaction()
        {
            return this.connection.BeginTransaction();
        }

        public IDbTransaction BeginTransaction(IsolationLevel il)
        {
            return this.connection.BeginTransaction(il);
        }

        public void ChangeDatabase(string databaseName)
        {
            this.connection.ChangeDatabase(databaseName);
        }

        public void Close()
        {
            this.connection.Close();
        }

        public IDbCommand CreateCommand()
        {
            return this.connection.CreateCommand();
        }

        public void Open()
        {
            this.connection.Open();
        }

        public string ConnectionString
        {
            get => this.connection.ConnectionString;
            set => this.connection.ConnectionString = value;
        }

        public int ConnectionTimeout => this.connection.ConnectionTimeout;

        public string Database => this.connection.Database;

        public ConnectionState State => this.connection.State;
    }
}