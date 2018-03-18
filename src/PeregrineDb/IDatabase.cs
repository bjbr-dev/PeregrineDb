namespace PeregrineDb
{
    using System.Data;

    public interface IDatabase
        : IDatabaseConnection
    {
        IDatabaseUnitOfWork StartUnitOfWork();

        IDatabaseUnitOfWork StartUnitOfWork(IsolationLevel isolationLevel);
    }
}