namespace PeregrineDb.Tests.Schema
{
    using System.ComponentModel.DataAnnotations.Schema;
    using FluentAssertions;
    using PeregrineDb.Schema;
    using PeregrineDb.Tests.Utils;
    using Xunit;

    public class DefaultTableNameFactoryTests
    {
        public class GetTableName
            : DefaultTableNameFactoryTests
        {
            private readonly AtttributeTableNameConvention sut = new AtttributeTableNameConvention(new TestSqlNameEscaper());

            [Fact]
            public void Returns_name_in_TableAttribute()
            {
                // Act
                var result = this.sut.GetTableName(typeof(TableWtihAttribute));

                // Assert
                result.Should().Be("'Tables'");
            }

            [Fact]
            public void Returns_name_and_schema_in_TableAttribute()
            {
                // Act
                var result = this.sut.GetTableName(typeof(TableWtihSchema));

                // Assert
                result.Should().Be("'Schema'.'Tables'");
            }

            [Fact]
            public void Pluralizes_name_of_table()
            {
                // Act
                var result = this.sut.GetTableName(typeof(SimpleTable));

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