namespace PeregrineDb.Tests.Schema
{
    using System;
    using System.ComponentModel.DataAnnotations.Schema;
    using System.Reflection;
    using FluentAssertions;
    using PeregrineDb.Schema;
    using Xunit;

    public class DefaultColumnNameFactoryTests
    {
        public class GetColumnName
            : DefaultColumnNameFactoryTests
        {
            private readonly AttributeColumnNameFactory sut = new AttributeColumnNameFactory();

            [Fact]
            public void Returns_name_of_property()
            {
                // Arrange
                var makePropertySchema = MakePropertySchema(typeof(SimpleProperty), nameof(SimpleProperty.Property));

                // Act
                var result = this.sut.GetColumnName(makePropertySchema);

                // Assert
                result.Should().Be("Property");
            }

            [Fact]
            public void Returns_name_in_columnAttribute()
            {
                // Arrange
                var makePropertySchema = MakePropertySchema(
                    typeof(PropertyWtihAttribute),
                    nameof(PropertyWtihAttribute.Property));

                // Act
                var result = this.sut.GetColumnName(makePropertySchema);

                // Assert
                result.Should().Be("ActualProperty");
            }

            private static PropertySchema MakePropertySchema(Type type, string propertyName)
            {
                return PropertySchema.MakePropertySchema(type.GetProperty(propertyName));
            }

            private class SimpleProperty
            {
                public int Property { get; set; }
            }

            private class PropertyWtihAttribute
            {
                [Column("ActualProperty")]
                public int Property { get; set; }
            }
        }
    }
}