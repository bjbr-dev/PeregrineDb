namespace PeregrineDb
{
    using System.Data;

    public interface ISqlConnection<out TConnection>
        : ISqlConnection
        where TConnection: IDbConnection
    {
        new TConnection DbConnection { get; }
    }
}