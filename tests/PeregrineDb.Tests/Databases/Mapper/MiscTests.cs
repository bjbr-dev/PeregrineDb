#if NETCOREAPP1_0
using System.Collections;
using System.Dynamic;
using System.Data.SqlTypes;
#else // net452

#endif

#if NETCOREAPP1_0
namespace System
{
    public enum GenericUriParserOptions
    {
        Default
    }

    public class GenericUriParser
    {
        private readonly GenericUriParserOptions options;

        public GenericUriParser(GenericUriParserOptions options)
        {
            this.options = options;
        }
    }
}
#endif

namespace PeregrineDb.Tests.Databases.Mapper
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using Microsoft.CSharp.RuntimeBinder;
    using PeregrineDb.Databases.Mapper;
    using PeregrineDb.Tests.Databases.Mapper.Helpers;
    using PeregrineDb.Tests.Databases.Mapper.SharedTypes;
    using PeregrineDb.Tests.Utils;
    using Xunit;

    public class MiscTests
    {
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

        [Fact]
        public void TestStructAsParam()
        {
            using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
            {
                var car1 = new CarWithAllProps { Name = "Ford", Age = 21, Trap = Car.TrapEnum.B };
                // note Car has Name as a field; parameters only respect properties at the moment
                var command = new SqlCommand("select @Name Name, @Age Age, @Trap Trap", car1);
                var car2 = database.Query<CarWithAllProps>(in command).First();

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
        public void TestSchemaChangedViaFirstOrDefault()
        {
            using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
            {
                database.Execute($"create table #dog(Age int, Name nvarchar(max)) insert #dog values(1, 'Alf')");
                try
                {
                    var d = database.QueryFirstOrDefault<Dog>($"select * from #dog");
                    Assert.Equal("Alf", d.Name);
                    Assert.Equal(1, d.Age);
                    database.Execute($"alter table #dog drop column Name");
                    d = database.QueryFirstOrDefault<Dog>($"select * from #dog");
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
        public void Test_Single_First_Default()
        {
            using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
            {
                FormattableString sql = $"select 0 where 1 = 0;"; // no rows

                var ex = Assert.Throws<InvalidOperationException>(() => database.QueryFirst<int>(sql));
                Assert.Equal("Sequence contains no elements", ex.Message);

                ex = Assert.Throws<InvalidOperationException>(() => database.QuerySingle<int>(sql));
                Assert.Equal("Sequence contains no elements", ex.Message);

                Assert.Equal(0, database.QueryFirstOrDefault<int>(sql));
                Assert.Equal(0, database.QuerySingleOrDefault<int>(sql));

                sql = $"select 1;"; // one row
                Assert.Equal(1, database.QueryFirst<int>(sql));
                Assert.Equal(1, database.QuerySingle<int>(sql));
                Assert.Equal(1, database.QueryFirstOrDefault<int>(sql));
                Assert.Equal(1, database.QuerySingleOrDefault<int>(sql));

                sql = $"select 2 union select 3 order by 1;"; // two rows
                Assert.Equal(2, database.QueryFirst<int>(sql));

                ex = Assert.Throws<InvalidOperationException>(() => database.QuerySingle<int>(sql));
                Assert.Equal("Sequence contains more than one element", ex.Message);

                Assert.Equal(2, database.QueryFirstOrDefault<int>(sql));

                ex = Assert.Throws<InvalidOperationException>(() => database.QuerySingleOrDefault<int>(sql));
                Assert.Equal("Sequence contains more than one element", ex.Message);
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

        // see https://stackoverflow.com/questions/16726709/string-format-with-sql-wildcard-causing-dapper-query-to-break
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
        public void TestExpando()
        {
            using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
            {
                var rows = database.Query<dynamic>($"select 1 A, 2 B union all select 3, 4").ToList();

                Assert.Equal(1, (int)rows[0].A);
                Assert.Equal(2, (int)rows[0].B);
                Assert.Equal(3, (int)rows[1].A);
                Assert.Equal(4, (int)rows[1].B);
            }
        }

        [Fact]
        public void TestStringList()
        {
            using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
            {
                Assert.Equal(
                    new[] { "a", "b", "c" },
                    database.Query<string>($"select * from (select 'a' as x union all select 'b' union all select 'c') as T where x in {new[] { "a", "b", "c" }}")
                );
                Assert.Equal(
                    new string[0],
                    database.Query<string>($"select * from (select 'a' as x union all select 'b' union all select 'c') as T where x in {new string[0]}")
                );
            }
        }

        [Fact]
        public void TestExecuteCommand()
        {
            using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
            {
                var result = database.Execute($@"
    set nocount on 
    create table #t(i int) 
    set nocount off 
    insert #t 
    select {1} a union all select {2}
    set nocount on 
    drop table #t");
                Assert.Equal(2, result.NumRowsAffected);
            }
        }

        [Fact]
        public void TestExecuteMultipleCommand()
        {
            using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
            {
                database.Execute($"create table #t(i int)");
                try
                {
                    var command = new SqlCommand("insert #t (i) values(@a)", new[] { new { a = 1 }, new { a = 2 }, new { a = 3 }, new { a = 4 } });

                    var tally = database.Execute(in command);
                    var sum = database.Query<int>($"select sum(i) from #t").First();
                    Assert.Equal(4, tally.NumRowsAffected);
                    Assert.Equal(10, sum);
                }
                finally
                {
                    database.Execute($"drop table #t");
                }
            }
        }

        private class Student
        {
            public string Name { get; set; }
            public int Age { get; set; }
        }

        [Fact]
        public void TestExecuteMultipleCommandStrongType()
        {
            using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
            {
                database.Execute($"create table #t(Name nvarchar(max), Age int)");
                try
                {
                    var command = new SqlCommand($"insert #t (Name,Age) values(@Name, @Age)", new List<Student>
                        {
                            new Student { Age = 1, Name = "sam" },
                            new Student { Age = 2, Name = "bob" }
                        });
                    var tally = database.Execute(in command);
                    int sum = database.Query<int>($"select sum(Age) from #t").First();
                    Assert.Equal(2, tally.NumRowsAffected);
                    Assert.Equal(3, sum);
                }
                finally
                {
                    database.Execute($"drop table #t");
                }
            }
        }

        [Fact]
        public void TestExecuteMultipleCommandObjectArray()
        {
            using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
            {
                database.Execute($"create table #t(i int)");
                var command = new SqlCommand("insert #t (i) values(@a)", new object[] { new { a = 1 }, new { a = 2 }, new { a = 3 }, new { a = 4 } });
                var tally = database.Execute(in command);
                int sum = database.Query<int>($"select sum(i) from #t drop table #t").First();
                Assert.Equal(4, tally.NumRowsAffected);
                Assert.Equal(10, sum);
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
        public void TestExpandWithNullableFields()
        {
            using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
            {
                var row = database.Query<dynamic>($"select null A, 2 B").Single();
                Assert.Null((int?)row.A);
                Assert.Equal(2, (int?)row.B);
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

                var obj = database.Query<dynamic>(
                                      $"select datalength({a}) as a, datalength({b}) as b, datalength({c}) as c, datalength({d}) as d, datalength({e}) as e, datalength({f}) as f")
                                  .First();
                Assert.Equal(10, (int)obj.a);
                Assert.Equal(20, (int)obj.b);
                Assert.Equal(5, (int)obj.c);
                Assert.Equal(10, (int)obj.d);
                Assert.Equal(5, (int)obj.e);
                Assert.Equal(10, (int)obj.f);
            }
        }

        [Fact]
        public void TestDefaultDbStringDbType()
        {
            var origDefaultStringDbType = DbString.IsAnsiDefault;
            try
            {
                DbString.IsAnsiDefault = true;
                var a = new DbString { Value = "abcde" };
                var b = new DbString { Value = "abcde", IsAnsi = false };
                Assert.True(a.IsAnsi);
                Assert.False(b.IsAnsi);
            }
            finally
            {
                DbString.IsAnsiDefault = origDefaultStringDbType;
            }
        }

        [Fact]
        public void TestFastExpandoSupportsIDictionary()
        {
            using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
            {
                var row = database.Query<dynamic>($"select 1 A, 'two' B").First() as IDictionary<string, object>;
                Assert.Equal(1, row["A"]);
                Assert.Equal("two", row["B"]);
            }
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

        //[Fact]
        //public void TestUnexpectedDataMessage()
        //{
        //    string msg = null;
        //    try
        //    {
        //        database.Query<int>("select count(1) where 1 = @Foo", new WithBizarreData { Foo = new System.GenericUriParser(GenericUriParserOptions.Default), Bar = 23 }).First();
        //    }
        //    catch (Exception ex)
        //    {
        //        msg = ex.Message;
        //    }
        //    Assert.Equal("The member Foo of type System.GenericUriParser cannot be used as a parameter value", msg);
        //}

        //[Fact]
        //public void TestUnexpectedButFilteredDataMessage()
        //{
        //    int i = database.Query<int>("select @Bar", new WithBizarreData { Foo = new GenericUriParser(GenericUriParserOptions.Default), Bar = 23 }).Single();

        //    Assert.Equal(23, i);
        //}

        //private class WithBizarreData
        //{
        //    public GenericUriParser Foo { get; set; }
        //    public int Bar { get; set; }
        //}

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

        [Fact]
        public void TestDynamicMutation()
        {
            using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
            {
                var obj = database.Query<dynamic>($"select 1 as [a], 2 as [b], 3 as [c]").Single();
                Assert.Equal(1, (int)obj.a);
                IDictionary<string, object> dict = obj;
                Assert.Equal(3, dict.Count);
                Assert.True(dict.Remove("a"));
                Assert.False(dict.Remove("d"));
                Assert.Equal(2, dict.Count);
                dict.Add("d", 4);
                Assert.Equal(3, dict.Count);
                Assert.Equal("b,c,d", string.Join(",", dict.Keys.OrderBy(x => x)));
                Assert.Equal("2,3,4", string.Join(",", dict.OrderBy(x => x.Key).Select(x => x.Value)));

                Assert.Equal(2, (int)obj.b);
                Assert.Equal(3, (int)obj.c);
                Assert.Equal(4, (int)obj.d);
                try
                {
                    Assert.Equal(1, (int)obj.a);
                    throw new InvalidOperationException("should have thrown");
                }
                catch (RuntimeBinderException)
                {
                    // pass
                }
            }
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
        public void TestDapperTableMetadataRetrieval()
        {
            using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
            {
                // Test for a bug found in CS 51509960 where the following sequence would result in an InvalidOperationException being
                // thrown due to an attempt to access a disposed of DataReader:
                //
                // - Perform a dynamic query that yields no results
                // - Add data to the source of that query
                // - Perform a the same query again
                database.Execute($"CREATE TABLE #sut (value varchar(10) NOT NULL PRIMARY KEY)");
                Assert.Equal(Enumerable.Empty<dynamic>(), database.Query<dynamic>($"SELECT value FROM #sut"));

                Assert.Equal(1, database.Execute($"INSERT INTO #sut (value) VALUES ('test')").NumRowsAffected);
                var result = database.Query<dynamic>($"SELECT value FROM #sut");

                var first = result.First();
                Assert.Equal("test", (string)first.value);
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
        public void Issue22_ExecuteScalar()
        {
            using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
            {
                int i = database.ExecuteScalar<int>($"select 123");
                Assert.Equal(123, i);

                i = database.ExecuteScalar<int>($"select cast(123 as bigint)");
                Assert.Equal(123, i);

                long j = database.ExecuteScalar<long>($"select 123");
                Assert.Equal(123L, j);

                j = database.ExecuteScalar<long>($"select cast(123 as bigint)");
                Assert.Equal(123L, j);

                int? k = database.ExecuteScalar<int?>($"select {default(int?)}");
                Assert.Null(k);
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
        public void QueryBasicWithoutQuery()
        {
            using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
            {
                int? i = database.Query<int?>($"print 'not a query'").FirstOrDefault();
                Assert.Null(i);
            }
        }

        [Fact]
        public void QueryComplexWithoutQuery()
        {
            using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
            {
                var obj = database.Query<Foo1>($"print 'not a query'").FirstOrDefault();
                Assert.Null(obj);
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

                    var command2 = new SqlCommand(sql, parameters);
                    var rowcount = (int)database.Query<dynamic>(in command2).Single().Foo;
                    Assert.Equal(1, rowcount);
                });
                Assert.Equal("An enumerable sequence of parameters (arrays, lists, etc) is not allowed in this context", ex.Message);
            }
        }

        [Fact]
        public async void SO35470588_WrongValuePidValue()
        {
            using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
            {
                // nuke, rebuild, and populate the table
                try
                {
                    database.Execute($"drop table TPTable");
                }
                catch
                {
                    /* don't care */
                }

                database.Execute($@"
create table TPTable (Pid int not null primary key identity(1,1), Value int not null);
insert TPTable (Value) values (2), (568)");

                // fetch the data using the query in the question, then force to a dictionary
                var rows = (await database.QueryAsync<TPTable>($"select * from TPTable").ConfigureAwait(false))
                    .ToDictionary(x => x.Pid);

                // check the number of rows
                Assert.Equal(2, rows.Count);

                // check row 1
                var row = rows[1];
                Assert.Equal(1, row.Pid);
                Assert.Equal(2, row.Value);

                // check row 2
                row = rows[2];
                Assert.Equal(2, row.Pid);
                Assert.Equal(568, row.Value);
            }
        }

        public class TPTable
        {
            public int Pid { get; set; }
            public int Value { get; set; }
        }

        [Fact]
        public void GetOnlyProperties()
        {
            using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
            {
                var obj = database.QuerySingle<HazGetOnly>($"select 42 as [Id], 'def' as [Name];");
                Assert.Equal(42, obj.Id);
                Assert.Equal("def", obj.Name);
            }
        }

        private class HazGetOnly
        {
            public int Id { get; }
            public string Name { get; } = "abc";
        }
    }
}
