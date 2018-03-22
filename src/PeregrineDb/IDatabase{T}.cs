namespace PeregrineDb
{
    using System.Data;

    public interface IDatabase<out TConnection>
        : IDatabase, IDatabaseConnection<TConnection>
        where TConnection : IDbConnection
    {
    }
}