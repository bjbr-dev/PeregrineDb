namespace PeregrineDb.Tests.Databases
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Data;
    using System.Data.SqlClient;
    using System.Diagnostics;
    using System.Dynamic;
    using System.Globalization;
    using System.Linq;
    using FluentAssertions;
    using PeregrineDb.Databases.Mapper;
    using PeregrineDb.Mapping;
    using PeregrineDb.Tests.Databases.Mapper.Helpers;
    using PeregrineDb.Tests.Databases.Mapper.SharedTypes;
    using PeregrineDb.Tests.Utils;
    using Xunit;
    using DynamicParameters = PeregrineDb.Mapping.DynamicParameters;
    using SqlClientCommand = System.Data.SqlClient.SqlCommand;
    using SqlCommand = PeregrineDb.SqlCommand;

    public enum UnhandledTypeOptions
    {
        Default
    }

    public class UnhandledType
    {
        private readonly UnhandledTypeOptions options;

        public UnhandledType(UnhandledTypeOptions options)
        {
            this.options = options;
        }
    }

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

            public class Parameters
            {
                private class DbParams
                    : IDynamicParameters, IEnumerable<IDbDataParameter>
                {
                    private readonly List<IDbDataParameter> parameters = new List<IDbDataParameter>();

                    public IEnumerator<IDbDataParameter> GetEnumerator()
                    {
                        return this.parameters.GetEnumerator();
                    }

                    IEnumerator IEnumerable.GetEnumerator()
                    {
                        return this.GetEnumerator();
                    }

                    public void Add(IDbDataParameter value)
                    {
                        this.parameters.Add(value);
                    }

                    void IDynamicParameters.AddParameters(IDbCommand command, Identity identity)
                    {
                        foreach (var parameter in this.parameters)
                        {
                            command.Parameters.Add(parameter);
                        }
                    }
                }

                [Fact]
                public void TestCustomParameters()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        var args = new DbParams
                            {
                                new SqlParameter("foo", 123),
                                new SqlParameter("bar", "abc")
                            };

                        var result = database.RawQuery<TestCustomParametersEntity>("select Foo=@foo, Bar=@bar", args).Single();
                        Assert.Equal(123, result.Foo);
                        Assert.Equal("abc", result.Bar);
                    }
                }

                private class TestCustomParametersEntity
                {
                    public int Foo { get; set; }

                    public string Bar { get; set; }
                }

                [Fact]
                public void TestDoubleParam()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        Assert.Equal(0.1d, database.Query<double>($"select {0.1d}").First());
                    }
                }

                [Fact]
                public void TestBoolParam()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        Assert.False(database.Query<bool>($"select {false}").First());
                    }
                }

                /// <summary>
                /// https://connect.microsoft.com/VisualStudio/feedback/details/381934/sqlparameter-dbtype-dbtype-time-sets-the-parameter-to-sqldbtype-datetime-instead-of-sqldbtype-time
                /// </summary>
                [Fact]
                public void TestTimeSpanParam()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        Assert.Equal(database.Query<TimeSpan>($"select {TimeSpan.FromMinutes(42)}").First(), TimeSpan.FromMinutes(42));
                    }
                }

                [Fact]
                public void PassInIntArray()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.PostgreSql))
                    {
                        Assert.Equal(
                            new[] { 1, 2, 3 },
                            database.Query<int>(
                                $"select * from (select 1 as Id union all select 2 union all select 3) as X where Id = ANY ({new[] { 1, 2, 3 }.AsEnumerable()})")
                        );
                    }
                }

                [Fact]
                public void PassInEmptyIntArray()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.PostgreSql))
                    {
                        Assert.Equal(
                            new int[0],
                            database.Query<int>($"select * from (select 1 as Id union all select 2 union all select 3) as X where Id = ANY ({new int[0]})")
                        );
                    }
                }

                [Fact]
                public void TestMassiveStrings()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        var str = new string('X', 20000);
                        Assert.Equal(database.Query<string>($"select {str}").First(), str);
                    }
                }

                [Fact]
                public void TestNullableGuidSupport()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        var guid = database.Query<Guid?>($"select null").First();
                        Assert.Null(guid);

                        guid = Guid.NewGuid();
                        var guid2 = database.Query<Guid?>($"select {guid}").First();
                        Assert.Equal(guid, guid2);
                    }
                }

                [Fact]
                public void TestNonNullableGuidSupport()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        var guid = Guid.NewGuid();
                        var guid2 = database.Query<Guid?>($"select {guid}").First();
                        Assert.True(guid == guid2);
                    }
                }

                [Fact]
                public void TestParameterInclusionNotSensitiveToCurrentCulture()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        // note this might fail if your database server is case-sensitive
                        var current = CultureInfo.CurrentCulture;
                        try
                        {
                            CultureInfo.CurrentCulture = new CultureInfo("tr-TR");

                            database.RawQuery<int>("select @pid", new { PId = 1 }).Single();
                        }
                        finally
                        {
                            CultureInfo.CurrentCulture = current;
                        }
                    }
                }

                [Fact]
                public void TestAppendingAnonClasses()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        var p = new DynamicParameters();
                        p.AddDynamicParams(new { A = 1, B = 2 });
                        p.AddDynamicParams(new { C = 3, D = 4 });

                        var result = database.RawQuery<TestAppendingAnonClassesEntity>("select @A a,@B b,@C c,@D d", p).Single();

                        Assert.Equal(1, result.A);
                        Assert.Equal(2, result.B);
                        Assert.Equal(3, result.C);
                        Assert.Equal(4, result.D);
                    }
                }

                private class TestAppendingAnonClassesEntity
                {
                    public int A { get; set; }

                    public int B { get; set; }

                    public int C { get; set; }

                    public int D { get; set; }
                }

                [Fact]
                public void TestAppendingADictionary()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        var dictionary = new Dictionary<string, object>
                            {
                                ["A"] = 1,
                                ["B"] = "two"
                            };

                        var p = new DynamicParameters();
                        p.AddDynamicParams(dictionary);

                        var result = database.RawQuery<TestAppendingADictionaryEntity>("select @A a, @B b", p).Single();

                        Assert.Equal(1, result.a);
                        Assert.Equal("two", result.b);
                    }
                }

                private class TestAppendingADictionaryEntity
                {
                    public int a { get; set; }

                    public string b { get; set; }
                }

                [Fact(Skip = "Not working")]
                public void TestAppendingAnExpandoObject()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        dynamic expando = new ExpandoObject();
                        expando.A = 1;
                        expando.B = "two";

                        var p = new DynamicParameters();
                        p.AddDynamicParams(expando);

                        var result = database.RawQuery<dynamic>("select @A a, @B b", p).Single();

                        Assert.Equal(1, (int)result.a);
                        Assert.Equal("two", (string)result.b);
                    }
                }
                [Fact]
                public void TestAppendingAList()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.PostgreSql))
                    {
                        var p = new DynamicParameters();
                        var list = new int[] { 1, 2, 3 };
                        p.AddDynamicParams(new { list });

                        var result = database.RawQuery<int>("select * from (select 1 A union all select 2 union all select 3) X where A = ANY (@list)", p).ToList();

                        Assert.Equal(1, result[0]);
                        Assert.Equal(2, result[1]);
                        Assert.Equal(3, result[2]);
                    }
                }

                [Fact]
                public void TestAppendingAListAsDictionary()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.PostgreSql))
                    {
                        var p = new DynamicParameters();
                        var list = new int[] { 1, 2, 3 };
                        var args = new Dictionary<string, object> { ["ids"] = list };
                        p.AddDynamicParams(args);

                        var result = database.RawQuery<int>("select * from (select 1 A union all select 2 union all select 3) X where A = ANY (@ids)", p).ToList();

                        Assert.Equal(1, result[0]);
                        Assert.Equal(2, result[1]);
                        Assert.Equal(3, result[2]);
                    }
                }

                [Fact]
                public void TestAppendingAListByName()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.PostgreSql))
                    {
                        DynamicParameters p = new DynamicParameters();
                        var list = new int[] { 1, 2, 3 };
                        p.Add("ids", list);

                        var result = database.RawQuery<int>("select * from (select 1 A union all select 2 union all select 3) X where A = ANY (@ids)", p).ToList();

                        Assert.Equal(1, result[0]);
                        Assert.Equal(2, result[1]);
                        Assert.Equal(3, result[2]);
                    }
                }

                [Fact]
                public void TestProcedureWithTimeParameter()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        var p = new DynamicParameters();
                        p.Add("a", TimeSpan.FromHours(10), dbType: DbType.Time);

                        database.Execute($@"CREATE PROCEDURE #TestProcWithTimeParameter
            @a TIME
            AS 
            BEGIN
            SELECT @a
            END");

                        Assert.Equal(database.RawQuery<TimeSpan>("#TestProcWithTimeParameter", p, CommandType.StoredProcedure).First(),
                            new TimeSpan(10, 0, 0));
                    }
                }

                [Fact]
                public void TestUniqueIdentifier()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        var guid = Guid.NewGuid();
                        var result = database.Query<Guid>($"declare @foo uniqueidentifier set @foo = {guid} select @foo").Single();
                        Assert.Equal(guid, result);
                    }
                }

                [Fact]
                public void TestNullableUniqueIdentifierNonNull()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        Guid? guid = Guid.NewGuid();
                        var result = database.Query<Guid?>($"declare @foo uniqueidentifier set @foo = {guid} select @foo").Single();
                        Assert.Equal(guid, result);
                    }
                }

                [Fact]
                public void TestNullableUniqueIdentifierNull()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        Guid? guid = null;
                        var result = database.Query<Guid?>($"declare @foo uniqueidentifier set @foo = {guid} select @foo").Single();
                        Assert.Equal(guid, result);
                    }
                }

                [Fact]
                public void TestSupportForDynamicParameters()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        var p = new DynamicParameters();
                        p.Add("name", "bob");
                        p.Add("age", dbType: DbType.Int32, direction: ParameterDirection.Output);

                        Assert.Equal("bob", database.RawQuery<string>("set @age = 11 select @name", p).First());
                        Assert.Equal(11, p.Get<int>("age"));
                    }
                }

                [Fact]
                public void TestSupportForExpandoObjectParameters()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        dynamic p = new ExpandoObject();
                        p.name = "bob";
                        object parameters = p;
                        string result = database.RawQuery<string>("select @name", parameters).First();
                        Assert.Equal("bob", result);
                    }
                }

                [Fact]
                public void SO25297173_DynamicIn()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.PostgreSql))
                    {
                        var queryParams = new Dictionary<string, object>
                            {
                                ["myIds"] = new[] { 5, 6 }
                            };

                        var dynamicParams = new DynamicParameters(queryParams);
                        var result = database.RawQuery<int>(@"SELECT id FROM unnest (@myIds) as id", dynamicParams);
                        result.ShouldAllBeEquivalentTo(new[] { 5, 6 });
                    }
                }

                [Fact]
                public void Test_AddDynamicParametersRepeatedShouldWork()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        var args = new DynamicParameters();
                        args.AddDynamicParams(new { Foo = 123 });
                        args.AddDynamicParams(new { Foo = 123 });
                        int i = database.RawQuery<int>("select @Foo", args).Single();
                        Assert.Equal(123, i);
                    }
                }

                [Fact]
                public void Test_AddDynamicParametersRepeatedIfParamTypeIsDbStiringShouldWork()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        var foo = new DbString { Value = "123" };

                        var args = new DynamicParameters();
                        args.AddDynamicParams(new { Foo = foo });
                        args.AddDynamicParams(new { Foo = foo });
                        int i = database.RawQuery<int>("select @Foo", args).Single();
                        Assert.Equal(123, i);
                    }
                }

                [Fact]
                public void AllowIDictionaryParameters()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        var parameters = new Dictionary<string, object>
                        {
                            ["param1"] = 0
                        };

                        database.RawQuery<dynamic>("SELECT @param1", parameters);
                    }
                }
                [Fact]
                public void TestParameterWithIndexer()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        database.Execute($@"create proc #TestProcWithIndexer 
                	@A int
                as 
                begin
                	select @A
                end");

                        var item = database.RawQuery<int>("#TestProcWithIndexer", new ParameterWithIndexer(), CommandType.StoredProcedure).Single();
                    }
                }

                public class ParameterWithIndexer
                {
                    public int A { get; set; }
                    public virtual string this[string columnName]
                    {
                        get { return null; }
                        set { }
                    }
                }

                [Fact]
                public void TestMultipleParametersWithIndexer()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        var order = database.Query<MultipleParametersWithIndexer>($"select 1 A,2 B").First();

                        Assert.Equal(1, order.A);
                        Assert.Equal(2, order.B);
                    }
                }

                public class MultipleParametersWithIndexer : MultipleParametersWithIndexerDeclaringType
                {
                    public int A { get; set; }
                }

                public class MultipleParametersWithIndexerDeclaringType
                {
                    public object this[object field]
                    {
                        get { return null; }
                        set { }
                    }

                    public object this[object field, int index]
                    {
                        get { return null; }
                        set { }
                    }

                    public int B { get; set; }
                }

                [Fact]
                public void Issue182_BindDynamicObjectParametersAndColumns()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        database.Execute($"create table #Dyno ([Id] uniqueidentifier primary key, [Name] nvarchar(50) not null, [Foo] bigint not null);");

                        var guid = Guid.NewGuid();
                        var orig = new Dyno { Name = "T Rex", Id = guid, Foo = 123L };
                        var result = database.RawExecute("insert into #Dyno ([Id], [Name], [Foo]) values (@Id, @Name, @Foo);", orig);

                        var fromDb = database.Query<Dyno>($"select * from #Dyno where Id={guid}").Single();
                        Assert.Equal((Guid)fromDb.Id, guid);
                        Assert.Equal("T Rex", fromDb.Name);
                        Assert.Equal(123L, (long)fromDb.Foo);
                    }
                }

                public class Dyno
                {
                    public dynamic Id { get; set; }
                    public string Name { get; set; }

                    public object Foo { get; set; }
                }

                [Fact]
                public void Issue220_InParameterCanBeSpecifiedInAnyCase()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.PostgreSql))
                    {
                        // note this might fail if your database server is case-sensitive
                        Assert.Equal(new[] { 1 },
                            database.RawQuery<int>("select * from (select 1 as Id) as X where Id = ANY (@ids)", new { Ids = new[] { 1 } }));
                    }
                }

                [Fact]
                public void TestStructs()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        var car = database.Query<Car>($"select 'Ford' Name, 21 Age, 2 Trap").First();

                        Assert.Equal(21, car.Age);
                        Assert.Equal("Ford", car.Name);
                        Assert.Equal(2, (int)car.Trap);
                    }
                }
                private struct Car
                {
                    public enum TrapEnum : int
                    {
                        A = 1,
                        B = 2
                    }
#pragma warning disable 0649
                    public string Name;
#pragma warning restore 0649
                    public int Age { get; set; }
                    public TrapEnum Trap { get; set; }
                }

                private struct CarWithAllProps
                {
                    public string Name { get; set; }
                    public int Age { get; set; }

                    public Car.TrapEnum Trap { get; set; }
                }

                [Fact]
                public void TestStructAsParam()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        var car1 = new CarWithAllProps { Name = "Ford", Age = 21, Trap = Car.TrapEnum.B };
                        // note Car has Name as a field; parameters only respect properties at the moment
                        var car2 = database.RawQuery<CarWithAllProps>("select @Name Name, @Age Age, @Trap Trap", car1).First();

                        Assert.Equal(car2.Name, car1.Name);
                        Assert.Equal(car2.Age, car1.Age);
                        Assert.Equal(car2.Trap, car1.Trap);
                    }
                }

                [Fact]
                public void SelectListInt()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        Assert.Equal(new[] { 1, 2, 3 }, database.Query<int>($"select 1 union all select 2 union all select 3"));
                    }
                }

                [Fact]
                public void SelectBinary()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        database.Query<byte[]>($"select cast(1 as varbinary(4))").First().SequenceEqual(new byte[] { 1 });
                    }
                }

                [Fact]
                public void TestSchemaChanged()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        database.Execute($"create table #dog(Age int, Name nvarchar(max)) insert #dog values(1, 'Alf')");
                        try
                        {
                            var d = database.Query<Dog>($"select * from #dog").Single();
                            Assert.Equal("Alf", d.Name);
                            Assert.Equal(1, d.Age);
                            database.Execute($"alter table #dog drop column Name");
                            d = database.Query<Dog>($"select * from #dog").Single();
                            Assert.Null(d.Name);
                            Assert.Equal(1, d.Age);
                        }
                        finally
                        {
                            database.Execute($"drop table #dog");
                        }
                    }
                }

                [Fact]
                public void TestStrings()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        Assert.Equal(new[] { "a", "b" }, database.Query<string>($"select 'a' a union select 'b'"));
                    }
                }

                /// <summary>
                /// see https://stackoverflow.com/questions/16726709/string-format-with-sql-wildcard-causing-dapper-query-to-break
                /// </summary>
                [Fact]
                public void CheckComplexConcat()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        FormattableString end_wildcard = $@"
SELECT * FROM #users16726709
WHERE (first_name LIKE CONCAT({"F"}, '%') OR last_name LIKE CONCAT({"F"}, '%'));";

                        FormattableString both_wildcards = $@"
SELECT * FROM #users16726709
WHERE (first_name LIKE CONCAT('%', {"F"}, '%') OR last_name LIKE CONCAT('%', {"F"}, '%'));";

                        FormattableString formatted = $@"
SELECT * FROM #users16726709
WHERE (first_name LIKE CONCAT({"F"}, '%') OR last_name LIKE CONCAT({"F"}, '%'));";

                        // if true, slower query due to not being able to use indices, but will allow searching inside strings 

                        database.Execute($@"create table #users16726709 (first_name varchar(200), last_name varchar(200))
insert #users16726709 values ('Fred','Bloggs') insert #users16726709 values ('Tony','Farcus') insert #users16726709 values ('Albert','TenoF')");

                        // Using Dapper
                        Assert.Equal(2, database.Query<dynamic>(end_wildcard).Count());
                        Assert.Equal(3, database.Query<dynamic>(both_wildcards).Count());
                        Assert.Equal(2, database.Query<dynamic>(formatted).Count());
                    }
                }
                [Fact]
                public void TestExtraFields()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        var guid = Guid.NewGuid();
                        var dog = database.Query<Dog>($"select '' as Extra, 1 as Age, 0.1 as Name1 , Id = {guid}");

                        Assert.Single(dog);
                        Assert.Equal(1, dog.First().Age);
                        Assert.Equal(dog.First().Id, guid);
                    }
                }

                [Fact]
                public void TestStrongType()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        var guid = Guid.NewGuid();
                        var dog = database.Query<Dog>($"select Age = {(int?)null}, Id = {guid}");

                        Assert.Single(dog);
                        Assert.Null(dog.First().Age);
                        Assert.Equal(dog.First().Id, guid);
                    }
                }

                [Fact]
                public void TestSimpleNull()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        Assert.Null(database.Query<DateTime?>($"select null").First());
                    }
                }

                [Fact]
                public void TestStringList()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.PostgreSql))
                    {
                        var values = new[] { "a", "b", "c" }.ToList();
                        database.Query<string>(
                                    $@"
select * from (select 'a' as x union all select 'b' union all select 'c') as T 
where x = ANY ({values})")
                                .ShouldAllBeEquivalentTo(values);

                        var emptyList = new string[0].ToList();
                        database.Query<string>($"select * from (select 'a' as x union all select 'b' union all select 'c') as T where x = ANY ({emptyList})")
                                .Should().BeEmpty();
                    }
                }

                private class TestObj
                {
                    public int _internal;
                    internal int Internal
                    {
                        set { this._internal = value; }
                    }

                    public int _priv;
                    private int Priv
                    {
                        set { this._priv = value; }
                    }

                    private int PrivGet => this._priv;
                }

                [Fact]
                public void TestSetInternal()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        Assert.Equal(10, database.Query<TestObj>($"select 10 as [Internal]").First()._internal);
                    }
                }

                [Fact]
                public void TestSetPrivate()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        Assert.Equal(10, database.Query<TestObj>($"select 10 as [Priv]").First()._priv);
                    }
                }

                [Fact]
                public void TestNakedBigInt()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        const long foo = 12345;
                        var result = database.Query<long>($"select {foo}").Single();
                        Assert.Equal(foo, result);
                    }
                }

                [Fact]
                public void TestBigIntMember()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        const long foo = 12345;
                        var result = database.Query<WithBigInt>($@"
declare @bar table(Value bigint)
insert @bar values ({foo})
select * from @bar").Single();
                        Assert.Equal(result.Value, foo);
                    }
                }

                private class WithBigInt
                {
                    public long Value { get; set; }
                }

                [Fact]
                public void TestFieldsAndPrivates()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        var data = database.Query<TestFieldCaseAndPrivatesEntity>(
                            $"select a=1,b=2,c=3,d=4,f='5'").Single();
                        Assert.Equal(1, data.a);
                        Assert.Equal(2, data.GetB());
                        Assert.Equal(3, data.c);
                        Assert.Equal(4, data.GetD());
                        Assert.Equal(5, data.e);
                    }
                }

                private class TestFieldCaseAndPrivatesEntity
                {
#pragma warning disable IDE1006 // Naming Styles
                    public int a { get; set; }
                    private int b { get; set; }
                    public int GetB() { return this.b; }
                    public int c = 0;
#pragma warning disable RCS1169 // Mark field as read-only.
                    private int d = 0;
#pragma warning restore RCS1169 // Mark field as read-only.
                    public int GetD() { return this.d; }
                    public int e { get; set; }
                    private string f
                    {
                        get { return this.e.ToString(); }
                        set { this.e = int.Parse(value); }
                    }
#pragma warning restore IDE1006 // Naming Styles
                }

                private class InheritanceTest1
                {
                    public string Base1 { get; set; }
                    public string Base2 { get; private set; }
                }

                private class InheritanceTest2 : InheritanceTest1
                {
                    public string Derived1 { get; set; }
                    public string Derived2 { get; private set; }
                }

                [Fact]
                public void TestInheritance()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        // Test that inheritance works.
                        var list = database.Query<InheritanceTest2>($"select 'One' as Derived1, 'Two' as Derived2, 'Three' as Base1, 'Four' as Base2");
                        Assert.Equal("One", list.First().Derived1);
                        Assert.Equal("Two", list.First().Derived2);
                        Assert.Equal("Three", list.First().Base1);
                        Assert.Equal("Four", list.First().Base2);
                    }
                }

                [Fact]
                public void TestDbString()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        var a = new DbString { Value = "abcde", IsFixedLength = true, Length = 10, IsAnsi = true };
                        var b = new DbString { Value = "abcde", IsFixedLength = true, Length = 10, IsAnsi = false };
                        var c = new DbString { Value = "abcde", IsFixedLength = false, Length = 10, IsAnsi = true };
                        var d = new DbString { Value = "abcde", IsFixedLength = false, Length = 10, IsAnsi = false };
                        var e = new DbString { Value = "abcde", IsAnsi = true };
                        var f = new DbString { Value = "abcde", IsAnsi = false };

                        var obj = database.Query<DbStringTestEntity>(
                                              $"select datalength({a}) as a, datalength({b}) as b, datalength({c}) as c, datalength({d}) as d, datalength({e}) as e, datalength({f}) as f")
                                          .First();

                        obj.ShouldBeEquivalentTo(new DbStringTestEntity
                            {
                                A = 10,
                                B = 20,
                                C = 5,
                                D = 10,
                                E = 5,
                                F = 10
                            });
                    }
                }

                private class DbStringTestEntity
                {
                    public int A { get; set; }

                    public int B { get; set; }

                    public int C { get; set; }

                    public int D { get; set; }

                    public int E { get; set; }

                    public int F { get; set; }
                }

                [Fact]
                public void TestDapperSetsPrivates()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        Assert.Equal(1, database.Query<PrivateDan>($"select 'one' ShadowInDB").First().Shadow);

                        Assert.Equal(1, database.QueryFirstOrDefault<PrivateDan>($"select 'one' ShadowInDB").Shadow);
                    }
                }

                private class PrivateDan
                {
                    public int Shadow { get; set; }
                    private string ShadowInDB
                    {
                        set { this.Shadow = value == "one" ? 1 : 0; }
                    }
                }
                private class WithCharValue
                {
                    public char Value { get; set; }
                    public char? ValueNullable { get; set; }
                }

                [Fact]
                public void TestCharInputAndOutput()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        const char test = '〠';
                        char c = database.Query<char>($"select {test}").Single();

                        Assert.Equal(c, test);

                        var obj = database.Query<WithCharValue>($"select {c} as Value").Single();

                        Assert.Equal(obj.Value, test);
                    }
                }

                [Fact]
                public void TestNullableCharInputAndOutputNonNull()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        char? test = '〠';
                        char? c = database.Query<char?>($"select {test}").Single();

                        Assert.Equal(c, test);

                        var obj = database.Query<WithCharValue>($"select {c} as ValueNullable").Single();

                        Assert.Equal(obj.ValueNullable, test);
                    }
                }

                [Fact]
                public void TestNullableCharInputAndOutputNull()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        char? test = null;
                        char? c = database.Query<char?>($"select {test}").Single();

                        Assert.Equal(c, test);

                        var obj = database.Query<WithCharValue>($"select {c} as ValueNullable").Single();

                        Assert.Equal(obj.ValueNullable, test);
                    }
                }

                [Fact]
                public void WorkDespiteHavingWrongStructColumnTypes()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        var hazInt = database.Query<CanHazInt>($"select cast(1 as bigint) Value").Single();
                        hazInt.Value.Equals(1);
                    }
                }

                private struct CanHazInt
                {
                    public int Value { get; set; }
                }

                [Fact]
                public void TestInt16Usage()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        Assert.Equal(database.Query<short>($"select cast(42 as smallint)").Single(), (short)42);
                        Assert.Equal(database.Query<short?>($"select cast(42 as smallint)").Single(), (short?)42);
                        Assert.Equal(database.Query<short?>($"select cast(null as smallint)").Single(), (short?)null);

                        Assert.Equal(database.Query<ShortEnum>($"select cast(42 as smallint)").Single(), (ShortEnum)42);
                        Assert.Equal(database.Query<ShortEnum?>($"select cast(42 as smallint)").Single(), (ShortEnum?)42);
                        Assert.Equal(database.Query<ShortEnum?>($"select cast(null as smallint)").Single(), (ShortEnum?)null);

                        var row =
                            database.Query<WithInt16Values>(
                                    $"select cast(1 as smallint) as NonNullableInt16, cast(2 as smallint) as NullableInt16, cast(3 as smallint) as NonNullableInt16Enum, cast(4 as smallint) as NullableInt16Enum")
                                .Single();
                        Assert.Equal(row.NonNullableInt16, (short)1);
                        Assert.Equal(row.NullableInt16, (short)2);
                        Assert.Equal(ShortEnum.Three, row.NonNullableInt16Enum);
                        Assert.Equal(ShortEnum.Four, row.NullableInt16Enum);

                        row =
                            database.Query<WithInt16Values>(
                                    $"select cast(5 as smallint) as NonNullableInt16, cast(null as smallint) as NullableInt16, cast(6 as smallint) as NonNullableInt16Enum, cast(null as smallint) as NullableInt16Enum")
                                .Single();
                        Assert.Equal(row.NonNullableInt16, (short)5);
                        Assert.Equal(row.NullableInt16, (short?)null);
                        Assert.Equal(ShortEnum.Six, row.NonNullableInt16Enum);
                        Assert.Equal(row.NullableInt16Enum, (ShortEnum?)null);
                    }
                }

                [Fact]
                public void TestInt32Usage()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        Assert.Equal(database.Query<int>($"select cast(42 as int)").Single(), (int)42);
                        Assert.Equal(database.Query<int?>($"select cast(42 as int)").Single(), (int?)42);
                        Assert.Equal(database.Query<int?>($"select cast(null as int)").Single(), (int?)null);

                        Assert.Equal(database.Query<IntEnum>($"select cast(42 as int)").Single(), (IntEnum)42);
                        Assert.Equal(database.Query<IntEnum?>($"select cast(42 as int)").Single(), (IntEnum?)42);
                        Assert.Equal(database.Query<IntEnum?>($"select cast(null as int)").Single(), (IntEnum?)null);

                        var row =
                            database.Query<WithInt32Values>(
                                    $"select cast(1 as int) as NonNullableInt32, cast(2 as int) as NullableInt32, cast(3 as int) as NonNullableInt32Enum, cast(4 as int) as NullableInt32Enum")
                                .Single();
                        Assert.Equal(row.NonNullableInt32, (int)1);
                        Assert.Equal(row.NullableInt32, (int)2);
                        Assert.Equal(IntEnum.Three, row.NonNullableInt32Enum);
                        Assert.Equal(IntEnum.Four, row.NullableInt32Enum);

                        row =
                            database.Query<WithInt32Values>(
                                    $"select cast(5 as int) as NonNullableInt32, cast(null as int) as NullableInt32, cast(6 as int) as NonNullableInt32Enum, cast(null as int) as NullableInt32Enum")
                                .Single();
                        Assert.Equal(row.NonNullableInt32, (int)5);
                        Assert.Equal(row.NullableInt32, (int?)null);
                        Assert.Equal(IntEnum.Six, row.NonNullableInt32Enum);
                        Assert.Equal(row.NullableInt32Enum, (IntEnum?)null);
                    }
                }

                public class WithInt16Values
                {
                    public short NonNullableInt16 { get; set; }
                    public short? NullableInt16 { get; set; }
                    public ShortEnum NonNullableInt16Enum { get; set; }
                    public ShortEnum? NullableInt16Enum { get; set; }
                }

                public class WithInt32Values
                {
                    public int NonNullableInt32 { get; set; }
                    public int? NullableInt32 { get; set; }
                    public IntEnum NonNullableInt32Enum { get; set; }
                    public IntEnum? NullableInt32Enum { get; set; }
                }

                public enum IntEnum : int
                {
                    Zero = 0, One = 1, Two = 2, Three = 3, Four = 4, Five = 5, Six = 6
                }

                [Fact]
                public void Issue_40_AutomaticBoolConversion()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        var user = database.Query<Issue40_User>($"select UserId=1,Email='abc',Password='changeme',Active=cast(1 as tinyint)").Single();
                        Assert.True(user.Active);
                        Assert.Equal(1, user.UserID);
                        Assert.Equal("abc", user.Email);
                        Assert.Equal("changeme", user.Password);
                    }
                }

                public class Issue40_User
                {
                    public Issue40_User()
                    {
                        this.Email = this.Password = string.Empty;
                    }

                    public int UserID { get; set; }
                    public string Email { get; set; }
                    public string Password { get; set; }
                    public bool Active { get; set; }
                }

                // see https://stackoverflow.com/questions/13127886/dapper-returns-null-for-singleordefaultdatediff
                [Fact]
                public void TestNullFromInt_NoRows()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        var result = database.Query<int>( // case with rows
                                                 $"select DATEDIFF(day, GETUTCDATE(), {DateTime.UtcNow.AddDays(20)})")
                                             .SingleOrDefault();
                        Assert.Equal(20, result);

                        result = database.Query<int>( // case without rows
                                             $"select DATEDIFF(day, GETUTCDATE(), {DateTime.UtcNow.AddDays(20)}) where 1 = 0")
                                         .SingleOrDefault();
                        Assert.Equal(0, result); // zero rows; default of int over zero rows is zero
                    }
                }

                [Fact]
                public void DbStringAnsi()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        var a = database.Query<int>($"select datalength({new DbString { Value = "abc", IsAnsi = true }})").Single();
                        var b = database.Query<int>($"select datalength({new DbString { Value = "abc", IsAnsi = false }})").Single();
                        Assert.Equal(3, a);
                        Assert.Equal(6, b);
                    }
                }

                private class HasInt32
                {
                    public int Value { get; set; }
                }

                // https://stackoverflow.com/q/23696254/23354
                [Fact]
                public void DownwardIntegerConversion()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        FormattableString sql = $"select cast(42 as bigint) as Value";
                        int i = database.Query<HasInt32>(sql).Single().Value;
                        Assert.Equal(42, i);

                        i = database.Query<int>(sql).Single();
                        Assert.Equal(42, i);
                    }
                }

                private T CheetViaDynamic<T>(T template, FormattableString sql)
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        return database.Query<T>(sql).SingleOrDefault();
                    }
                }

                [Fact]
                public void Issue142_FailsNamedStatus()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        var row1 = database.Query<Issue142_Status>($"select {StatusType.Started} as [Status]").Single();
                        Assert.Equal(StatusType.Started, row1.Status);

                        var row2 = database.Query<Issue142_StatusType>($"select {StatusType.Started} as [Status]").Single();
                        Assert.Equal(Status.Started, row2.Status);
                    }
                }

                public class Issue142_Status
                {
                    public StatusType Status { get; set; }
                }

                public class Issue142_StatusType
                {
                    public Status Status { get; set; }
                }

                public enum StatusType : byte
                {
                    NotStarted = 1, Started = 2, Finished = 3
                }

                public enum Status : byte
                {
                    NotStarted = 1, Started = 2, Finished = 3
                }

                [Fact]
                public void TestUnexpectedDataMessage()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        string msg = null;
                        try
                        {
                            var i = database.RawQuery<int>("select count(1) where 1 = @Foo", new { Foo = new UnhandledType(UnhandledTypeOptions.Default) }).First();
                        }
                        catch (Exception ex)
                        {
                            msg = ex.Message;
                        }

                        Assert.Equal("The member Foo of type PeregrineDb.Tests.Databases.UnhandledType cannot be used as a parameter value", msg);
                    }
                }

                [Fact]
                public void TestUnexpectedButFilteredDataMessage()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        var i = database.RawQuery<int>("select @Bar", new { Foo = new UnhandledType(UnhandledTypeOptions.Default), Bar = 23 }).Single();

                        Assert.Equal(23, i);
                    }
                }

                [Fact]
                public void QueryBasicWithoutQuery()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        database.Query<int?>($"print 'not a query'").Should().BeEmpty();
                    }
                }

                [Fact]
                public void QueryComplexWithoutQuery()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        database.Query<Foo1>($"print 'not a query'").Should().BeEmpty();
                    }
                }

                [FactLongRunning]
                public void Issue263_Timeout()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        var watch = Stopwatch.StartNew();
                        var i = database.Query<int>($"waitfor delay '00:01:00'; select 42;", 300).Single();
                        watch.Stop();
                        Assert.Equal(42, i);
                        var minutes = watch.ElapsedMilliseconds / 1000 / 60;
                        Assert.True(minutes >= 0.95 && minutes <= 1.05);
                    }
                }

                [Fact]
                public void SO30435185_InvalidTypeOwner()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        var ex = Assert.Throws<InvalidOperationException>(() =>
                        {
                            const string sql = @"INSERT INTO #XXX
                        (XXXId, AnotherId, ThirdId, Value, Comment)
                        VALUES
                        (@XXXId, @AnotherId, @ThirdId, @Value, @Comment); select @@rowcount as [Foo]";

                            var command = new
                            {
                                MyModels = new[]
                                        {
                                    new { XXXId = 1, AnotherId = 2, ThirdId = 3, Value = "abc", Comment = "def" }
                                }
                            };
                            var parameters = command.MyModels
                                                    .Select(model => new
                                                    {
                                                        XXXId = model.XXXId,
                                                        AnotherId = model.AnotherId,
                                                        ThirdId = model.ThirdId,
                                                        Value = model.Value,
                                                        Comment = model.Comment
                                                    })
                                                    .ToArray();

                            var rowcount = (int)database.RawQuery<dynamic>(sql, parameters).Single().Foo;
                            Assert.Equal(1, rowcount);
                        });
                        Assert.Equal("An enumerable sequence of parameters (arrays, lists, etc) is not allowed in this context", ex.Message);
                    }
                }

                [Fact]
                public void TestNullableDefault()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        QueryCache.Purge();

                        var data = database.Query<NullTestClass>($@"
declare @data table(Id int not null, A int null, B int null, C varchar(20), D int null, E int null)
insert @data (Id, A, B, C, D, E) values 
	(1,null,null,null,null,null),
	(2,42,42,'abc',2,2)
select * from @data").ToDictionary(_ => _.Id);

                        var obj = data[2];

                        Assert.Equal(2, obj.Id);
                        Assert.Equal(42, obj.A);
                        Assert.Equal(42, obj.B);
                        Assert.Equal("abc", obj.C);
                        Assert.Equal(AnEnum.A, obj.D);
                        Assert.Equal(AnEnum.A, obj.E);

                        obj = data[1];
                        Assert.Equal(1, obj.Id);
                        Assert.Equal(2, obj.A);
                        Assert.Equal(2, obj.B);
                        Assert.Equal("def", obj.C);
                        Assert.Equal(AnEnum.B, obj.D);
                        Assert.Equal(AnEnum.B, obj.E);
                    }
                }

                private class NullTestClass
                {
                    public int Id { get; set; }
                    public int A { get; set; }
                    public int? B { get; set; }
                    public string C { get; set; }
                    public AnEnum D { get; set; }
                    public AnEnum? E { get; set; }

                    public NullTestClass()
                    {
                        this.A = 2;
                        this.B = 2;
                        this.C = "def";
                        this.D = AnEnum.B;
                        this.E = AnEnum.B;
                    }
                }
            }

            public class TableValuedParameters
            : Query
            {
                private static List<Microsoft.SqlServer.Server.SqlDataRecord> CreateSqlDataRecordList(IEnumerable<int> numbers)
                {
                    var numberList = new List<Microsoft.SqlServer.Server.SqlDataRecord>();

                    // Create an SqlMetaData object that describes our table type.
                    Microsoft.SqlServer.Server.SqlMetaData[] tvpDefinition = { new Microsoft.SqlServer.Server.SqlMetaData("n", SqlDbType.Int) };

                    foreach (var n in numbers)
                    {
                        // Create a new record, using the metadata array above.
                        var rec = new Microsoft.SqlServer.Server.SqlDataRecord(tvpDefinition);
                        rec.SetInt32(0, n); // Set the value.
                        numberList.Add(rec); // Add it to the list.
                    }

                    return numberList;
                }

                private class IntDynamicParam : IDynamicParameters
                {
                    private readonly IEnumerable<int> numbers;

                    public IntDynamicParam(IEnumerable<int> numbers)
                    {
                        this.numbers = numbers;
                    }

                    public void AddParameters(IDbCommand command, Identity identity)
                    {
                        var sqlCommand = (SqlClientCommand)command;
                        sqlCommand.CommandType = CommandType.StoredProcedure;

                        var number_list = CreateSqlDataRecordList(this.numbers);

                        // Add the table parameter.
                        var p = sqlCommand.Parameters.Add("ints", SqlDbType.Structured);
                        p.Direction = ParameterDirection.Input;
                        p.TypeName = "int_list_type";
                        p.Value = number_list;
                    }
                }

                private class IntCustomParam
                    : ICustomQueryParameter
                {
                    private readonly IEnumerable<int> numbers;

                    public IntCustomParam(IEnumerable<int> numbers)
                    {
                        this.numbers = numbers;
                    }

                    public void AddParameter(IDbCommand command, string name)
                    {
                        var sqlCommand = (SqlClientCommand)command;
                        sqlCommand.CommandType = CommandType.StoredProcedure;

                        var numberList = CreateSqlDataRecordList(this.numbers);

                        // Add the table parameter.
                        var p = sqlCommand.Parameters.Add(name, SqlDbType.Structured);
                        p.Direction = ParameterDirection.Input;
                        p.TypeName = "int_list_type";
                        p.Value = numberList;
                    }
                }

                [Fact]
                public void TestTVPWithAnonymousObject()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        try
                        {
                            database.Execute($"CREATE TYPE int_list_type AS TABLE (n int NOT NULL PRIMARY KEY)");
                            database.Execute($"CREATE PROC get_ints @integers int_list_type READONLY AS select * from @integers");

                            var nums = database.RawQuery<int>("get_ints", new { integers = new IntCustomParam(new[] { 1, 2, 3 }) }, CommandType.StoredProcedure);
                            Assert.Equal(1, nums[0]);
                            Assert.Equal(2, nums[1]);
                            Assert.Equal(3, nums[2]);
                            Assert.Equal(3, nums.Count);
                        }
                        finally
                        {
                            try
                            {
                                database.Execute($"DROP PROC get_ints");
                            }
                            finally
                            {
                                database.Execute($"DROP TYPE int_list_type");
                            }
                        }
                    }
                }

                // SQL Server specific test to demonstrate TVP 
                [Fact]
                public void TestTVP()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        try
                        {
                            database.Execute($"CREATE TYPE int_list_type AS TABLE (n int NOT NULL PRIMARY KEY)");
                            database.Execute($"CREATE PROC get_ints @ints int_list_type READONLY AS select * from @ints");

                            var nums = database.RawQuery<int>("get_ints", new IntDynamicParam(new[] { 1, 2, 3 })).ToList();
                            Assert.Equal(1, nums[0]);
                            Assert.Equal(2, nums[1]);
                            Assert.Equal(3, nums[2]);
                            Assert.Equal(3, nums.Count);
                        }
                        finally
                        {
                            try
                            {
                                database.Execute($"DROP PROC get_ints");
                            }
                            finally
                            {
                                database.Execute($"DROP TYPE int_list_type");
                            }
                        }
                    }
                }
            }
        }
    }
}