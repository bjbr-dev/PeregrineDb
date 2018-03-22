namespace PeregrineDb.Databases
{
    using System.Data;

    public class DefaultDatabase<TConnection>
        : DefaultDatabase, IDatabase<TConnection>
        where TConnection : IDbConnection
    {
        public DefaultDatabase(TConnection connection, PeregrineConfig config)
            : base(connection, config)
        {
            this.DbConnection = connection;
        }

        public new TConnection DbConnection { get; }
    }
}