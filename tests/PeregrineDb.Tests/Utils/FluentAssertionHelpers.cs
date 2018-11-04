namespace PeregrineDb.Tests.Utils
{
    using System.Collections.Generic;
    using FluentAssertions;
    using FluentAssertions.Equivalency;
    using FluentAssertions.Primitives;

    public static class FluentAssertionHelpers
    {
        public static TSelf UsingComparer<TSelf, TProperty>(this SelfReferenceEquivalencyAssertionOptions<TSelf> self, IEqualityComparer<TProperty> comparer)
            where TSelf : SelfReferenceEquivalencyAssertionOptions<TSelf>
        {
            return self.Using<TProperty>(a => comparer.Equals(a.Expectation, a.Subject).Should().BeTrue()).WhenTypeIs<TProperty>();
        }

        public static AndConstraint<StringAssertions> Be(this StringAssertions assertions, string expected, IEqualityComparer<string> comparer)
        {
            comparer.Equals(assertions.Subject, expected).Should().BeTrue();
            return new AndConstraint<StringAssertions>(assertions);
        }

        public static SqlCommandAssertions Should(this SqlCommand instance)
        {
            return new SqlCommandAssertions(instance);
        }

        public class SqlCommandAssertions
            : ReferenceTypeAssertions<SqlCommand, SqlCommandAssertions>

        {
            public SqlCommandAssertions(SqlCommand instance)
            {
                this.Subject = instance;
            }

            public AndConstraint<SqlCommandAssertions> Be(SqlCommand expected)
            {
                var xSql = this.Subject.CommandText.Replace("\r\n", "\n").Trim();
                var ySql = expected.CommandText.Replace("\r\n", "\n").Trim();

                xSql.Should().Be(ySql);

                this.Subject.Parameters.Should().BeEquivalentTo(expected.Parameters);
                return new AndConstraint<SqlCommandAssertions>(this);
            }

            protected override string Identifier => "SqlCommand";
        }
    }
}
