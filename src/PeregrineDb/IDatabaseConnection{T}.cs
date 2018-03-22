namespace PeregrineDb
{
    using System.Data;

    public interface IDatabaseConnection<out TConnection>
        : IDatabaseConnection
        where TConnection: IDbConnection
    {
        new TConnection DbConnection { get; }
    }
}