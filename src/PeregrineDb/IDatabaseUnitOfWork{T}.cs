namespace PeregrineDb
{
    using System.Data;

    public interface IDatabaseUnitOfWork<out TConnection, out TTransaction>
        : IDatabaseUnitOfWork, IDatabaseConnection<TConnection>
        where TConnection : IDbConnection
        where TTransaction : IDbTransaction
    {
        new TTransaction Transaction { get; }
    }
}