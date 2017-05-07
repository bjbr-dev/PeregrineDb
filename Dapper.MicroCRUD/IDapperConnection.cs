// ReSharper disable once CheckNamespace
namespace Dapper
{
    using System.Data;
    using Dapper.MicroCRUD.Dialects;

    public interface IDapperConnection
    {
        IDbConnection DbConnection { get; }

        IDbTransaction Transaction { get; }

        IDialect Dialect { get; }
    }
}