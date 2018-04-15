namespace PeregrineDb.Tests.Databases
{
    using System;
    using System.Linq;
    using PeregrineDb.Databases.Mapper;
    using PeregrineDb.Tests.Databases.Mapper.Helpers;
    using PeregrineDb.Tests.Databases.Mapper.SharedTypes;
    using PeregrineDb.Tests.Utils;
    using Xunit;

    public abstract partial class DefaultDatabaseConnectionStatementsTests
    {
        public abstract class Query
            : DefaultDatabaseConnectionStatementsTests
        {
            public class Constructors
                : Query
            {
                [Fact]
                public void TestAbstractInheritance()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        var order = database.Query<AbstractInheritance.ConcreteOrder>($"select 1 Internal,2 Protected,3 [Public],4 Concrete").First();

                        Assert.Equal(1, order.Internal);
                        Assert.Equal(2, order.ProtectedVal);
                        Assert.Equal(3, order.Public);
                        Assert.Equal(4, order.Concrete);
                    }
                }

                [Fact]
                public void TestMultipleConstructors()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        var mult = database.Query<MultipleConstructors>($"select 0 A, 'Dapper' b").First();
                        Assert.Equal(0, mult.A);
                        Assert.Equal("Dapper", mult.B);
                    }
                }

                [Fact]
                public void TestConstructorsWithAccessModifiers()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        var value = database.Query<ConstructorsWithAccessModifiers>($"select 0 A, 'Dapper' b").First();
                        Assert.Equal(1, value.A);
                        Assert.Equal("Dapper!", value.B);
                    }
                }

                [Fact]
                public void TestNoDefaultConstructor()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        var guid = Guid.NewGuid();
                        var nodef = database.Query<NoDefaultConstructor>(
                                                $"select CAST(NULL AS integer) A1,  CAST(NULL AS integer) b1, CAST(NULL AS real) f1, 'Dapper' s1, G1 = {guid}")
                                            .First();
                        Assert.Equal(0, nodef.A);
                        Assert.Null(nodef.B);
                        Assert.Equal(0, nodef.F);
                        Assert.Equal("Dapper", nodef.S);
                        Assert.Equal(nodef.G, guid);
                    }
                }

                [Fact]
                public void TestNoDefaultConstructorWithChar()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        const char c1 = 'ą';
                        const char c3 = 'ó';
                        var nodef = database.Query<NoDefaultConstructorWithChar>($"select {c1} c1, {default(char?)} c2, {c3} c3").First();
                        Assert.Equal(nodef.Char1, c1);
                        Assert.Null(nodef.Char2);
                        Assert.Equal(nodef.Char3, c3);
                    }
                }

                [Fact]
                public void TestNoDefaultConstructorWithEnum()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        var nodef = database.Query<NoDefaultConstructorWithEnum>(
                                                $"select cast(2 as smallint) E1, cast(5 as smallint) n1, cast(null as smallint) n2")
                                            .First();
                        Assert.Equal(ShortEnum.Two, nodef.E);
                        Assert.Equal(ShortEnum.Five, nodef.NE1);
                        Assert.Null(nodef.NE2);
                    }
                }

                [Fact]
                public void ExplicitConstructors()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        var rows = database.Query<_ExplicitConstructors>($@"
declare @ExplicitConstructors table (
    Field INT NOT NULL PRIMARY KEY IDENTITY(1,1),
    Field_1 INT NOT NULL);
insert @ExplicitConstructors(Field_1) values (1);
SELECT * FROM @ExplicitConstructors"
                        ).ToList();

                        Assert.Single(rows);
                        Assert.Equal(1, rows[0].Field);
                        Assert.Equal(1, rows[0].Field_1);
                        Assert.True(rows[0].GetWentThroughProperConstructor());
                    }
                }

                [Fact]
                public void TestWithNonPublicConstructor()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        var output = database.Query<WithPrivateConstructor>($"select 1 as Foo").First();
                        Assert.Equal(1, output.Foo);
                    }
                }

                private class _ExplicitConstructors
                {
                    public int Field { get; set; }
                    public int Field_1 { get; set; }

                    private readonly bool WentThroughProperConstructor;

                    public _ExplicitConstructors()
                    {
                        /* yep */
                    }

                    [ExplicitConstructor]
                    public _ExplicitConstructors(string foo, int bar)
                    {
                        this.WentThroughProperConstructor = true;
                    }

                    public bool GetWentThroughProperConstructor()
                    {
                        return this.WentThroughProperConstructor;
                    }
                }

                public static class AbstractInheritance
                {
                    public abstract class Order
                    {
                        internal int Internal { get; set; }
                        protected int Protected { get; set; }
                        public int Public { get; set; }

                        public int ProtectedVal => this.Protected;
                    }

                    public class ConcreteOrder : Order
                    {
                        public int Concrete { get; set; }
                    }
                }

                private class MultipleConstructors
                {
                    public MultipleConstructors()
                    {
                    }

                    public MultipleConstructors(int a, string b)
                    {
                        this.A = a + 1;
                        this.B = b + "!";
                    }

                    public int A { get; set; }
                    public string B { get; set; }
                }

                private class ConstructorsWithAccessModifiers
                {
                    private ConstructorsWithAccessModifiers()
                    {
                    }

                    public ConstructorsWithAccessModifiers(int a, string b)
                    {
                        this.A = a + 1;
                        this.B = b + "!";
                    }

                    public int A { get; set; }
                    public string B { get; set; }
                }

                private class NoDefaultConstructor
                {
                    public NoDefaultConstructor(int a1, int? b1, float f1, string s1, Guid G1)
                    {
                        this.A = a1;
                        this.B = b1;
                        this.F = f1;
                        this.S = s1;
                        this.G = G1;
                    }

                    public int A { get; set; }
                    public int? B { get; set; }
                    public float F { get; set; }
                    public string S { get; set; }
                    public Guid G { get; set; }
                }

                private class NoDefaultConstructorWithChar
                {
                    public NoDefaultConstructorWithChar(char c1, char? c2, char? c3)
                    {
                        this.Char1 = c1;
                        this.Char2 = c2;
                        this.Char3 = c3;
                    }

                    public char Char1 { get; set; }
                    public char? Char2 { get; set; }
                    public char? Char3 { get; set; }
                }

                private class NoDefaultConstructorWithEnum
                {
                    public NoDefaultConstructorWithEnum(ShortEnum e1, ShortEnum? n1, ShortEnum? n2)
                    {
                        this.E = e1;
                        this.NE1 = n1;
                        this.NE2 = n2;
                    }

                    public ShortEnum E { get; set; }
                    public ShortEnum? NE1 { get; set; }
                    public ShortEnum? NE2 { get; set; }
                }

                private class WithPrivateConstructor
                {
                    public int Foo { get; set; }

                    private WithPrivateConstructor()
                    {
                    }
                }
            }

            public class Decimals
                : Execute
            {
                [Fact]
                public void BasicDecimals()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        var c = database.Query<decimal>($"select {11.884M}").Single();
                        Assert.Equal(11.884M, c);
                    }
                }

                [Fact]
                public void TestDoubleDecimalConversions_SO18228523_RightWay()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        var row = database.Query<HasDoubleDecimal>(
                            $"select cast(1 as float) as A, cast(2 as float) as B, cast(3 as decimal) as C, cast(4 as decimal) as D").Single();
                        row.A.Equals(1.0);
                        row.B.Equals(2.0);
                        row.C.Equals(3.0M);
                        row.D.Equals(4.0M);
                    }
                }

                [Fact]
                public void TestDoubleDecimalConversions_SO18228523_WrongWay()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        var row = database.Query<HasDoubleDecimal>(
                            $"select cast(1 as decimal) as A, cast(2 as decimal) as B, cast(3 as float) as C, cast(4 as float) as D").Single();
                        row.A.Equals(1.0);
                        row.B.Equals(2.0);
                        row.C.Equals(3.0M);
                        row.D.Equals(4.0M);
                    }
                }

                [Fact]
                public void TestDoubleDecimalConversions_SO18228523_Nulls()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        var row = database.Query<HasDoubleDecimal>(
                                          $"select cast(null as decimal) as A, cast(null as decimal) as B, cast(null as float) as C, cast(null as float) as D")
                                      .Single();
                        row.A.Equals(0.0);
                        Assert.Null(row.B);
                        row.C.Equals(0.0M);
                        Assert.Null(row.D);
                    }
                }

                private class HasDoubleDecimal
                {
                    public double A { get; set; }
                    public double? B { get; set; }
                    public decimal C { get; set; }
                    public decimal? D { get; set; }
                }
            }

            public class Enums
                : Query
            {
                [Fact]
                public void TestEnumWeirdness()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        Assert.Null(database.Query<TestEnumClass>($"select null as [EnumEnum]").First().EnumEnum);
                        Assert.Equal(TestEnum.Bla, database.Query<TestEnumClass>($"select cast(1 as tinyint) as [EnumEnum]").First().EnumEnum);
                    }
                }

                [Fact]
                public void TestEnumStrings()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        Assert.Equal(TestEnum.Bla, database.Query<TestEnumClassNoNull>($"select 'BLA' as [EnumEnum]").First().EnumEnum);
                        Assert.Equal(TestEnum.Bla, database.Query<TestEnumClassNoNull>($"select 'bla' as [EnumEnum]").First().EnumEnum);

                        Assert.Equal(TestEnum.Bla, database.Query<TestEnumClass>($"select 'BLA' as [EnumEnum]").First().EnumEnum);
                        Assert.Equal(TestEnum.Bla, database.Query<TestEnumClass>($"select 'bla' as [EnumEnum]").First().EnumEnum);
                    }
                }

                [Fact]
                public void TestEnumParamsWithNullable()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        const EnumParam a = EnumParam.A;
                        EnumParam? b = EnumParam.B, c = null;
                        var obj = database.Query<EnumParamObject>($"select {a} as A, {b} as B, {c} as C").Single();
                        Assert.Equal(EnumParam.A, obj.A);
                        Assert.Equal(EnumParam.B, obj.B);
                        Assert.Null(obj.C);
                    }
                }

                [Fact]
                public void TestEnumParamsWithoutNullable()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        const EnumParam a = EnumParam.A;
                        const EnumParam b = EnumParam.B, c = 0;
                        var obj = database.Query<EnumParamObjectNonNullable>($"select {a} as A, {b} as B, {c} as C").Single();
                        Assert.Equal(EnumParam.A, obj.A);
                        Assert.Equal(EnumParam.B, obj.B);
                        Assert.Equal(obj.C, (EnumParam)0);
                    }
                }

                [Fact]
                public void SO27024806_TestVarcharEnumMemberWithExplicitConstructor()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        var foo = database.Query<SO27024806Class>($"SELECT 'Foo' AS myField").Single();
                        Assert.Equal(SO27024806Enum.Foo, foo.MyField);
                    }
                }

                [Fact]
                public void DapperEnumValue_SqlServer()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        Common.DapperEnumValue(database);
                    }
                }

                private enum EnumParam : short
                {
                    None = 0,
                    A = 1,
                    B = 2
                }

                private class EnumParamObject
                {
                    public EnumParam A { get; set; }
                    public EnumParam? B { get; set; }
                    public EnumParam? C { get; set; }
                }

                private class EnumParamObjectNonNullable
                {
                    public EnumParam A { get; set; }
                    public EnumParam? B { get; set; }
                    public EnumParam? C { get; set; }
                }

                private enum TestEnum : byte
                {
                    Bla = 1
                }

                private class TestEnumClass
                {
                    public TestEnum? EnumEnum { get; set; }
                }

                private class TestEnumClassNoNull
                {
                    public TestEnum EnumEnum { get; set; }
                }

                private enum SO27024806Enum
                {
                    Foo = 0,
                    Bar = 1
                }

                private class SO27024806Class
                {
                    public SO27024806Class(SO27024806Enum myField)
                    {
                        this.MyField = myField;
                    }

                    public SO27024806Enum MyField { get; set; }
                }
            }
        }
    }
}