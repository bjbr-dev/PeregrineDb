namespace PeregrineDb
{
    using System.Data;

    public interface ISqlUnitOfWork
        : ISqlConnection
    {
        IDbTransaction Transaction { get; }

        void SaveChanges();

        void Rollback();
    }
}