// <copyright file="MicroCRUDConfigTests.cs" company="Berkeleybross">
// Copyright (c) Berkeleybross. All rights reserved.
// </copyright>
namespace Dapper.MicroCRUD.Tests
{
    using System;
    using Dapper.MicroCRUD.Schema;
    using Xunit;

    [Collection("MicroCRUDConfig")]
    public class MicroCRUDConfigTests
    {
        public class Misc
            : MicroCRUDConfigTests
        {
            [Fact]
            public void CanSetDefaultDialect()
            {
                // Act
                try
                {
                    MicroCRUDConfig.DefaultDialect = Dialect.PostgreSql;
                }
                finally
                {
                    MicroCRUDConfig.DefaultDialect = Dialect.SqlServer2012;
                }
            }

            [Fact]
            public void Can_set_TableNameFactory()
            {
                // Act
                try
                {
                    MicroCRUDConfig.SetTableNameFactory(new NonPluralizingTableNameFactory());
                }
                finally
                {
                    MicroCRUDConfig.SetTableNameFactory(new DefaultTableNameFactory());
                }
            }

            [Fact]
            public void Can_set_ColumnNameFactory()
            {
                // Act
                try
                {
                    MicroCRUDConfig.SetColumnNameFactory(new DefaultColumnNameFactory());
                }
                finally
                {
                    MicroCRUDConfig.SetColumnNameFactory(new DefaultColumnNameFactory());
                }
            }

            [Fact]
            public void Can_set_DefaultVerifyAffectedRowCount()
            {
                // Act
                try
                {
                    MicroCRUDConfig.DefaultVerifyAffectedRowCount = false;
                }
                finally
                {
                    MicroCRUDConfig.DefaultVerifyAffectedRowCount = true;
                }
            }
        }

        public class SetCurrent
            : MicroCRUDConfigTests
        {
            [Fact]
            public void Throws_exception_if_current_is_set_to_null()
            {
                // Act
                Assert.Throws<ArgumentException>(() => MicroCRUDConfig.SetCurrent(c => null));
            }
        }
    }
}