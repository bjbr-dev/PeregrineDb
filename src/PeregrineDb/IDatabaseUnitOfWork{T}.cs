namespace PeregrineDb
{
    using System.Data;

    public interface ISqlUnitOfWork<out TConnection, out TTransaction>
        : ISqlUnitOfWork, ISqlConnection<TConnection>
        where TConnection : IDbConnection
        where TTransaction : IDbTransaction
    {
        new TTransaction Transaction { get; }
    }
}