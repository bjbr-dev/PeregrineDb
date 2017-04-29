// <copyright file="SuffixRemovingTableNameFactoryTests.cs" company="Berkeleybross">
// Copyright (c) Berkeleybross. All rights reserved.
// </copyright>
namespace Dapper.MicroCRUD.Tests.Schema
{
    using System.ComponentModel.DataAnnotations.Schema;
    using Dapper.MicroCRUD.Dialects;
    using Dapper.MicroCRUD.Schema;
    using Dapper.MicroCRUD.Tests.Utils;
    using FluentAssertions;
    using Xunit;

    public class SuffixRemovingTableNameFactoryTests
    {
        public class GetTableName
            : SuffixRemovingTableNameFactoryTests
        {
            private readonly SuffixRemovingTableNameFactory sut = new SuffixRemovingTableNameFactory("Entity");
            private readonly IDialect dialect = new TestDialect();

            [Fact]
            public void Returns_name_in_TableAttribute()
            {
                // Act
                var result = this.sut.GetTableName(typeof(TableWtihAttribute), this.dialect);

                // Assert
                result.Should().Be("'Tables'");
            }

            [Fact]
            public void Returns_name_and_schema_in_TableAttribute()
            {
                // Act
                var result = this.sut.GetTableName(typeof(TableWtihSchema), this.dialect);

                // Assert
                result.Should().Be("'Schema'.'Tables'");
            }

            [Fact]
            public void Pluralizes_name_of_table()
            {
                // Act
                var result = this.sut.GetTableName(typeof(SimpleTable), this.dialect);

                // Assert
                result.Should().Be("'SimpleTables'");
            }

            [Fact]
            public void Removes_suffix()
            {
                // Act
                var result = this.sut.GetTableName(typeof(TableEntity), this.dialect);

                // Assert
                result.Should().Be("'Tables'");
            }

            [Fact]
            public void Removes_suffix_CI()
            {
                // Act
                var result = this.sut.GetTableName(typeof(TableENtity), this.dialect);

                // Assert
                result.Should().Be("'Tables'");
            }

            [Fact]
            public void Does_not_remove_suffix_if_the_entire_table_name_is_that_suffix()
            {
                // Act
                var result = this.sut.GetTableName(typeof(Entity), this.dialect);

                // Assert
                result.Should().Be("'Entitys'");
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