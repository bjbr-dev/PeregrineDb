namespace PeregrineDb
{
    using System.Data;

    public interface IDatabaseUnitOfWork
        : IDatabaseConnection
    {
        IDbTransaction Transaction { get; }

        void SaveChanges();

        void Rollback();
    }
}