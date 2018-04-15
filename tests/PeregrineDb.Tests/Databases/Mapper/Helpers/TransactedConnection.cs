namespace PeregrineDb.Tests.Databases.Mapper.Helpers
{
    using System;
    using System.Data;

    public class TransactedConnection
        : IDbConnection
    {
        private readonly IDbConnection _conn;
        private readonly IDbTransaction _tran;

        public TransactedConnection(IDbConnection conn, IDbTransaction tran)
        {
            this._conn = conn;
            this._tran = tran;
        }

        public string ConnectionString
        {
            get { return this._conn.ConnectionString; }
            set { this._conn.ConnectionString = value; }
        }

        public int ConnectionTimeout => this._conn.ConnectionTimeout;
        public string Database => this._conn.Database;
        public ConnectionState State => this._conn.State;

        public IDbTransaction BeginTransaction(IsolationLevel il)
        {
            throw new NotImplementedException();
        }

        public IDbTransaction BeginTransaction() => this._tran;

        public void ChangeDatabase(string databaseName) => this._conn.ChangeDatabase(databaseName);

        public void Close() => this._conn.Close();

        public IDbCommand CreateCommand()
        {
            // The command inherits the "current" transaction.
            var command = this._conn.CreateCommand();
            command.Transaction = this._tran;
            return command;
        }

        public void Dispose() => this._conn.Dispose();

        public void Open() => this._conn.Open();
    }
}
