namespace PeregrineDb.Tests
{
    using System;
    using PeregrineDb;
    using PeregrineDb.Schema;
    using Xunit;

    [Collection("PeregrineConfig")]
    public class DefautlConfigTests
    {
        public class Misc
            : DefautlConfigTests
        {
            [Fact]
            public void Can_set_Dialect()
            {
                try
                {
                    DefaultPeregrineConfig.Dialect = Dialect.PostgreSql;
                }
                finally
                {
                    DefaultPeregrineConfig.Reset();
                }
            }

            [Fact]
            public void Can_set_TableNameFactory()
            {
                try
                {
                    DefaultPeregrineConfig.TableNameFactory = new NonPluralizingTableNameFactory();
                }
                finally
                {
                    DefaultPeregrineConfig.Reset();
                }
            }

            [Fact]
            public void Can_set_ColumnNameFactory()
            {
                try
                {
                    DefaultPeregrineConfig.ColumnNameFactory = new DefaultColumnNameFactory();
                }
                finally
                {
                    DefaultPeregrineConfig.Reset();
                }
            }

            [Fact]
            public void Can_set_DefaultVerifyAffectedRowCount()
            {
                try
                {
                    DefaultPeregrineConfig.VerifyAffectedRowCount = false;
                }
                finally
                {
                    DefaultPeregrineConfig.Reset();
                }
            }
        }

        public class Update
            : DefautlConfigTests
        {
            [Fact]
            public void Throws_exception_if_update_function_returns_null()
            {
                // Act
                Assert.Throws<InvalidOperationException>(() => DefaultPeregrineConfig.Update(c => null));
            }
        }
    }
}