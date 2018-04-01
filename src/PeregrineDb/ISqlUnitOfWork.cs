namespace PeregrineDb
{
    using System.Data;

    public interface ISqlUnitOfWork<out TConnection, out TTransaction>
        : ISqlConnection<TConnection>
        where TConnection : IDbConnection
        where TTransaction : IDbTransaction
    {
        TTransaction Transaction { get; }

        void SaveChanges();

        void Rollback();
    }
}