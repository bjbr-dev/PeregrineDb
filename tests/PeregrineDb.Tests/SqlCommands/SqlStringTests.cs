namespace PeregrineDb.Tests.SqlCommands
{
    using System;
    using FluentAssertions;
    using Xunit;

    public class SqlStringTests
    {
        public class ParameterizePlaceholders
            : SqlStringTests
        {
            [Fact]
            public void Returns_same_sql_when_parameters_are_not_specified()
            {
                // Act
                var result = SqlString.ParameterizePlaceholders("SELECT * FROM foo", 0);

                // Assert
                result.Should().Be("SELECT * FROM foo");
            }

            [Fact]
            public void Replaces_first_placeholder_with_parameter()
            {
                // Act
                var result = SqlString.ParameterizePlaceholders("SELECT {0}", 1);

                // Assert
                result.Should().Be("SELECT @p0");
            }

            [Fact]
            public void Replaces_multiple_placeholders_with_parameter()
            {
                // Act
                var result = SqlString.ParameterizePlaceholders("SELECT {0}, {1}", 2);

                // Assert
                result.Should().Be("SELECT @p0, @p1");
            }

            [Fact]
            public void Allows_parameter_to_be_skipped()
            {
                // Act
                var result = SqlString.ParameterizePlaceholders("SELECT {1}", 2);

                // Assert
                result.Should().Be("SELECT @p1");
            }

            [Theory]
            [InlineData("SELECT {")]
            [InlineData("SELECT }")]
            [InlineData("SELECT {0")]
            [InlineData("SELECT {0 FROM foo")]
            [InlineData("SELECT 0} FROM foo")]
            [InlineData("{")]
            [InlineData("SELECT {foo} FROM bar")]
            [InlineData("SELECT {0 } FROM bar")]
            [InlineData("SELECT { 0} FROM bar")]
            [InlineData("SELECT {1} FROM bar")]
            [InlineData("SELECT {11} FROM bar")]
            public void Throws_error_if_placeholder_is_not_well_formed(string format)
            {
                // Act
                Action act = () => SqlString.ParameterizePlaceholders(format, 1);

                // Assert
                act.ShouldThrow<FormatException>();
            }

            [Theory]
            [InlineData("{{0}}", "{0}")]
            [InlineData("{{", "{")]
            [InlineData("}}", "}")]
            [InlineData("{{{0}}}", "{@p0}")]
            public void Allows_braces_to_be_escaped(string format, string expected)
            {
                // Act
                var result = SqlString.ParameterizePlaceholders(format, 1);

                // Assert
                result.Should().Be(expected);
            }

            [Theory]
            [InlineData("SELECT {0:x}")]
            [InlineData("SELECT {0,5}")]
            public void Throws_error_if_placeholder_specifies_formatting(string format)
            {
                // Act
                Action act = () => SqlString.ParameterizePlaceholders(format, 1);

                // Assert
                act.ShouldThrow<FormatException>();
            }
        }
    }
}