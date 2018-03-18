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
                    DefaultConfig.Dialect = Dialect.PostgreSql;
                }
                finally
                {
                    DefaultConfig.Reset();
                }
            }

            [Fact]
            public void Can_set_TableNameFactory()
            {
                try
                {
                    DefaultConfig.TableNameFactory = new NonPluralizingTableNameFactory();
                }
                finally
                {
                    DefaultConfig.Reset();
                }
            }

            [Fact]
            public void Can_set_ColumnNameFactory()
            {
                try
                {
                    DefaultConfig.ColumnNameFactory = new DefaultColumnNameFactory();
                }
                finally
                {
                    DefaultConfig.Reset();
                }
            }

            [Fact]
            public void Can_set_DefaultVerifyAffectedRowCount()
            {
                try
                {
                    DefaultConfig.VerifyAffectedRowCount = false;
                }
                finally
                {
                    DefaultConfig.Reset();
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
                Assert.Throws<InvalidOperationException>(() => DefaultConfig.Update(c => null));
            }
        }
    }
}