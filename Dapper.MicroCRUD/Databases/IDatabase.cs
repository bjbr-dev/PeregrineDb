namespace Dapper.MicroCRUD.Databases
{
    using System;
    using System.Data;

    public interface IDatabase
        : IDapperConnection, IDisposable
    {
        IUnitOfWork StartUnitOfWork();

        IUnitOfWork StartUnitOfWork(IsolationLevel isolationLevel);
    }
}