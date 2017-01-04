// <copyright file="SuffixRemovingTableNameFactoryTests.cs" company="Berkeleybross">
// Copyright (c) Berkeleybross. All rights reserved.
// </copyright>
namespace Dapper.MicroCRUD.Tests.Schema
{
    using System.ComponentModel.DataAnnotations.Schema;
    using Dapper.MicroCRUD.Schema;
    using NUnit.Framework;

    [TestFixture]
    public class SuffixRemovingTableNameFactoryTests
    {
        private class GetTableName
            : SuffixRemovingTableNameFactoryTests
        {
            private SuffixRemovingTableNameFactory sut;
            private Dialect dialect;

            [SetUp]
            public void SetUp()
            {
                this.sut = new SuffixRemovingTableNameFactory("Entity");
                this.dialect = new Dialect("Test", "GET IDENTITY", "'{0}'", "SKIP {0} TAKE {1}");
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

            [Test]
            public void Removes_suffix()
            {
                // Act
                var result = this.sut.GetTableName(typeof(TableEntity), this.dialect);

                // Assert
                Assert.AreEqual("'Tables'", result);
            }

            [Test]
            public void Removes_suffix_CI()
            {
                // Act
                var result = this.sut.GetTableName(typeof(TableENtity), this.dialect);

                // Assert
                Assert.AreEqual("'Tables'", result);
            }

            [Test]
            public void Does_not_remove_suffix_if_the_entire_table_name_is_that_suffix()
            {
                // Act
                var result = this.sut.GetTableName(typeof(Entity), this.dialect);

                // Assert
                Assert.AreEqual("'Entitys'", result);
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

            private class TableEntity
            {
            }

            private class TableENtity
            {
            }

            private class Entity
            {
            }
        }
    }
}