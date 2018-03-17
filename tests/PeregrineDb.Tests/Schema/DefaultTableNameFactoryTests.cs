// <copyright file="DefaultTableNameFactoryTests.cs" company="Berkeleybross">
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

    public class DefaultTableNameFactoryTests
    {
        public class GetTableName
            : DefaultTableNameFactoryTests
        {
            private readonly DefaultTableNameFactory sut = new DefaultTableNameFactory();
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