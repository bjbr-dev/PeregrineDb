namespace Dapper.MicroCRUD.Databases
{
    using System;

    public interface IUnitOfWork
        : IDapperConnection, IDisposable
    {
        void SaveChanges();

        void Rollback();
    }
}