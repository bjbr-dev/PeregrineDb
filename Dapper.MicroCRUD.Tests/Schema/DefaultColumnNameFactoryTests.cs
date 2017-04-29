// <copyright file="DefaultColumnNameFactoryTests.cs" company="Berkeleybross">
// Copyright (c) Berkeleybross. All rights reserved.
// </copyright>
namespace Dapper.MicroCRUD.Tests.Schema
{
    using System;
    using System.ComponentModel.DataAnnotations.Schema;
    using Dapper.MicroCRUD.Schema;
    using FluentAssertions;
    using Xunit;

    public class DefaultColumnNameFactoryTests
    {
        public class GetColumnName
            : DefaultColumnNameFactoryTests
        {
            private readonly DefaultColumnNameFactory sut = new DefaultColumnNameFactory();

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