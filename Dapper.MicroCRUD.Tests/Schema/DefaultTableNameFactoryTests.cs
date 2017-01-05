// <copyright file="DefaultTableNameFactoryTests.cs" company="Berkeleybross">
// Copyright (c) Berkeleybross. All rights reserved.
// </copyright>
namespace Dapper.MicroCRUD.Tests.Schema
{
    using System.ComponentModel.DataAnnotations.Schema;
    using Dapper.MicroCRUD.Dialects;
    using Dapper.MicroCRUD.Schema;
    using Dapper.MicroCRUD.Tests.Utils;
    using NUnit.Framework;

    [TestFixture]
    public class DefaultTableNameFactoryTests
    {
        private class GetTableName
            : DefaultTableNameFactoryTests
        {
            private DefaultTableNameFactory sut;
            private IDialect dialect;

            [SetUp]
            public void SetUp()
            {
                this.sut = new DefaultTableNameFactory();
                this.dialect = new TestDialect();
            }

            [Test]
            public void Returns_name_in_TableAttribute()
            {
                // Act
                var result = this.sut.GetTableName(typeof(TableWtihAttribute), this.dialect);

                // Assert
                Assert.AreEqual("'Tables'", result);
            }

            [Test]
            public void Returns_name_and_schema_in_TableAttribute()
            {
                // Act
                var result = this.sut.GetTableName(typeof(TableWtihSchema), this.dialect);

                // Assert
                Assert.AreEqual("'Schema'.'Tables'", result);
            }

            [Test]
            public void Pluralizes_name_of_table()
            {
                // Act
                var result = this.sut.GetTableName(typeof(SimpleTable), this.dialect);

                // Assert
                Assert.AreEqual("'SimpleTables'", result);
            }

            private class SimpleTable
            {
            }

            [Table("Tables")]
            private class TableWtihAttribute
            {
            }

            [Table("Tables", Schema = "Schema")]
            private class TableWtihSchema
            {
            }
        }
    }
}