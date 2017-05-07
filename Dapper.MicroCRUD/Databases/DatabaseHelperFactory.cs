using System;
using System.Collections.Generic;
using System.Text;

namespace Dapper.MicroCRUD.Databases
{
    public class DatabaseHelperFactory
    {
        public static THelper StartUnitOfWork<TSeed, THelper>(TSeed seed, Func<TSeed, IUnitOfWork> unitOfWorkFactory, Func<IUnitOfWork, THelper> helperFactory)
        {
            var unitOfWork = unitOfWorkFactory(seed);

            try
            {
                return helperFactory(unitOfWork);
            }
            catch
            {
                unitOfWork.Dispose();
                throw;
            }
        }

        public static THelper OpenDatabase<TSeed, THelper>(TSeed seed, Func<TSeed, IDatabase> databaseFactory, Func<IDatabase, THelper> helperFactory)
        {
            var database = databaseFactory(seed);

            try
            {
                return helperFactory(database);
            }
            catch
            {
                database.Dispose();
                throw;
            }
        }
    }
}
