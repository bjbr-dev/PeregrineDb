namespace PeregrineDb.Tests.Databases.Mapper
{
    using System;
    using System.Collections.Generic;
    using System.ComponentModel;
    using System.Data;
    using System.Linq;
    using System.Reflection;
    using System.Runtime.CompilerServices;
    using PeregrineDb.Databases.Mapper;
    using PeregrineDb.Mapping;
    using PeregrineDb.Tests.Databases.Mapper.Helpers;
    using PeregrineDb.Tests.Utils;
    using Xunit;

    public class TypeHandlerTests
    {
        [Fact]
        public void TestChangingDefaultStringTypeMappingToAnsiString()
        {
            using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
            {
                var result01 = database.Query<string>($"SELECT SQL_VARIANT_PROPERTY(CONVERT(sql_variant, {"TestString"}),'BaseType') AS BaseType").FirstOrDefault();
                Assert.Equal("nvarchar", result01);

                QueryCache.Purge();

                TypeProvider.AddTypeMap(typeof(string), DbType.AnsiString); // Change Default String Handling to AnsiString
                var result02 = database.Query<string>($"SELECT SQL_VARIANT_PROPERTY(CONVERT(sql_variant, {"TestString"}),'BaseType') AS BaseType").FirstOrDefault();
                Assert.Equal("varchar", result02);

                QueryCache.Purge();
                TypeProvider.AddTypeMap(typeof(string), DbType.String); // Restore Default to Unicode String
            }
        }

        [Fact]
        public void TestChangingDefaultStringTypeMappingToAnsiStringFirstOrDefault()
        {
            using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
            {
                var result01 = database.QueryFirstOrDefault<string>(
                    $"SELECT SQL_VARIANT_PROPERTY(CONVERT(sql_variant, {"TestString"}),'BaseType') AS BaseType");
                Assert.Equal("nvarchar", result01);

                QueryCache.Purge();

                TypeProvider.AddTypeMap(typeof(string), DbType.AnsiString); // Change Default String Handling to AnsiString
                var result02 = database.QueryFirstOrDefault<string>(
                    $"SELECT SQL_VARIANT_PROPERTY(CONVERT(sql_variant, {"TestString"}),'BaseType') AS BaseType");
                Assert.Equal("varchar", result02);

                QueryCache.Purge();
                TypeProvider.AddTypeMap(typeof(string), DbType.String); // Restore Default to Unicode String
            }
        }

        private static string GetDescriptionFromAttribute(MemberInfo member)
        {
            var data = member?.CustomAttributes.FirstOrDefault(x => x.AttributeType == typeof(DescriptionAttribute));
            return (string)data?.ConstructorArguments.Single().Value;
        }

        public class TypeWithMapping
        {
            [Description("B")]
            public string A { get; set; }

            [Description("A")]
            public string B { get; set; }
        }

        [Fact]
        public void Issue136_ValueTypeHandlers()
        {
            using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
            {
                TypeProvider.ResetTypeHandlers();
                TypeProvider.AddTypeHandler(typeof(LocalDate), LocalDateConverter.Default);
                var param = new LocalDateResult
                {
                    NotNullable = new LocalDate { Year = 2014, Month = 7, Day = 25 },
                    NullableNotNull = new LocalDate { Year = 2014, Month = 7, Day = 26 },
                    NullableIsNull = null,
                };

                var result = database.RawQuery<LocalDateResult>("SELECT @NotNullable AS NotNullable, @NullableNotNull AS NullableNotNull, @NullableIsNull AS NullableIsNull",
                    param).Single();

                TypeProvider.ResetTypeHandlers();
                TypeProvider.AddTypeHandler(typeof(LocalDate?), LocalDateConverter.Default);

                result = database.RawQuery<LocalDateResult>("SELECT @NotNullable AS NotNullable, @NullableNotNull AS NullableNotNull, @NullableIsNull AS NullableIsNull", param).Single();
            }
        }

        private class LocalDateConverter : DbTypeConverter<LocalDate>
        {
            private LocalDateConverter() { /* private constructor */ }

            // Make the field type ITypeHandler to ensure it cannot be used with SqlMapper.AddTypeHandler<T>(TypeHandler<T>)
            // by mistake.
            public static readonly IDbTypeConverter Default = new LocalDateConverter();

            public override LocalDate Parse(object value)
            {
                var date = (DateTime)value;
                return new LocalDate { Year = date.Year, Month = date.Month, Day = date.Day };
            }

            public override void SetValue(IDbDataParameter parameter, LocalDate value)
            {
                parameter.DbType = DbType.DateTime;
                parameter.Value = new DateTime(value.Year, value.Month, value.Day);
            }
        }

        public struct LocalDate
        {
            public int Year { get; set; }
            public int Month { get; set; }
            public int Day { get; set; }
        }

        public class LocalDateResult
        {
            public LocalDate NotNullable { get; set; }
            public LocalDate? NullableNotNull { get; set; }
            public LocalDate? NullableIsNull { get; set; }
        }

        public class LotsOfNumerics
        {
            public enum E_Byte : byte { A = 0, B = 1 }
            public enum E_SByte : sbyte { A = 0, B = 1 }
            public enum E_Short : short { A = 0, B = 1 }
            public enum E_UShort : ushort { A = 0, B = 1 }
            public enum E_Int : int { A = 0, B = 1 }
            public enum E_UInt : uint { A = 0, B = 1 }
            public enum E_Long : long { A = 0, B = 1 }
            public enum E_ULong : ulong { A = 0, B = 1 }

            public E_Byte P_Byte { get; set; }
            public E_SByte P_SByte { get; set; }
            public E_Short P_Short { get; set; }
            public E_UShort P_UShort { get; set; }
            public E_Int P_Int { get; set; }
            public E_UInt P_UInt { get; set; }
            public E_Long P_Long { get; set; }
            public E_ULong P_ULong { get; set; }

            public bool N_Bool { get; set; }
            public byte N_Byte { get; set; }
            public sbyte N_SByte { get; set; }
            public short N_Short { get; set; }
            public ushort N_UShort { get; set; }
            public int N_Int { get; set; }
            public uint N_UInt { get; set; }
            public long N_Long { get; set; }
            public ulong N_ULong { get; set; }

            public float N_Float { get; set; }
            public double N_Double { get; set; }
            public decimal N_Decimal { get; set; }

            public E_Byte? N_P_Byte { get; set; }
            public E_SByte? N_P_SByte { get; set; }
            public E_Short? N_P_Short { get; set; }
            public E_UShort? N_P_UShort { get; set; }
            public E_Int? N_P_Int { get; set; }
            public E_UInt? N_P_UInt { get; set; }
            public E_Long? N_P_Long { get; set; }
            public E_ULong? N_P_ULong { get; set; }

            public bool? N_N_Bool { get; set; }
            public byte? N_N_Byte { get; set; }
            public sbyte? N_N_SByte { get; set; }
            public short? N_N_Short { get; set; }
            public ushort? N_N_UShort { get; set; }
            public int? N_N_Int { get; set; }
            public uint? N_N_UInt { get; set; }
            public long? N_N_Long { get; set; }
            public ulong? N_N_ULong { get; set; }

            public float? N_N_Float { get; set; }
            public double? N_N_Double { get; set; }
            public decimal? N_N_Decimal { get; set; }
        }

        [Fact]
        public void TestBigIntForEverythingWorks()
        {
            this.TestBigIntForEverythingWorks_ByDataType<long>("bigint");
            this.TestBigIntForEverythingWorks_ByDataType<int>("int");
            this.TestBigIntForEverythingWorks_ByDataType<byte>("tinyint");
            this.TestBigIntForEverythingWorks_ByDataType<short>("smallint");
            this.TestBigIntForEverythingWorks_ByDataType<bool>("bit");
            this.TestBigIntForEverythingWorks_ByDataType<float>("float(24)");
            this.TestBigIntForEverythingWorks_ByDataType<double>("float(53)");
        }

        private void TestBigIntForEverythingWorks_ByDataType<T>(string dbType)
        {
            using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
            {
                string sql = "select " + string.Join(",", typeof(LotsOfNumerics).GetProperties().Select(
                                 x => "cast (1 as " + dbType + ") as [" + x.Name + "]"));
                var row = database.RawQuery<LotsOfNumerics>(sql, null).Single();

                Assert.True(row.N_Bool);
                Assert.Equal(row.N_SByte, (sbyte)1);
                Assert.Equal(row.N_Byte, (byte)1);
                Assert.Equal(row.N_Int, (int)1);
                Assert.Equal(row.N_UInt, (uint)1);
                Assert.Equal(row.N_Short, (short)1);
                Assert.Equal(row.N_UShort, (ushort)1);
                Assert.Equal(row.N_Long, (long)1);
                Assert.Equal(row.N_ULong, (ulong)1);
                Assert.Equal(row.N_Float, (float)1);
                Assert.Equal(row.N_Double, (double)1);
                Assert.Equal(row.N_Decimal, (decimal)1);

                Assert.Equal(LotsOfNumerics.E_Byte.B, row.P_Byte);
                Assert.Equal(LotsOfNumerics.E_SByte.B, row.P_SByte);
                Assert.Equal(LotsOfNumerics.E_Short.B, row.P_Short);
                Assert.Equal(LotsOfNumerics.E_UShort.B, row.P_UShort);
                Assert.Equal(LotsOfNumerics.E_Int.B, row.P_Int);
                Assert.Equal(LotsOfNumerics.E_UInt.B, row.P_UInt);
                Assert.Equal(LotsOfNumerics.E_Long.B, row.P_Long);
                Assert.Equal(LotsOfNumerics.E_ULong.B, row.P_ULong);

                Assert.True(row.N_N_Bool.Value);
                Assert.Equal(row.N_N_SByte.Value, (sbyte)1);
                Assert.Equal(row.N_N_Byte.Value, (byte)1);
                Assert.Equal(row.N_N_Int.Value, (int)1);
                Assert.Equal(row.N_N_UInt.Value, (uint)1);
                Assert.Equal(row.N_N_Short.Value, (short)1);
                Assert.Equal(row.N_N_UShort.Value, (ushort)1);
                Assert.Equal(row.N_N_Long.Value, (long)1);
                Assert.Equal(row.N_N_ULong.Value, (ulong)1);
                Assert.Equal(row.N_N_Float.Value, (float)1);
                Assert.Equal(row.N_N_Double.Value, (double)1);
                Assert.Equal(row.N_N_Decimal, (decimal)1);

                Assert.Equal(LotsOfNumerics.E_Byte.B, row.N_P_Byte.Value);
                Assert.Equal(LotsOfNumerics.E_SByte.B, row.N_P_SByte.Value);
                Assert.Equal(LotsOfNumerics.E_Short.B, row.N_P_Short.Value);
                Assert.Equal(LotsOfNumerics.E_UShort.B, row.N_P_UShort.Value);
                Assert.Equal(LotsOfNumerics.E_Int.B, row.N_P_Int.Value);
                Assert.Equal(LotsOfNumerics.E_UInt.B, row.N_P_UInt.Value);
                Assert.Equal(LotsOfNumerics.E_Long.B, row.N_P_Long.Value);
                Assert.Equal(LotsOfNumerics.E_ULong.B, row.N_P_ULong.Value);

                this.TestBigIntForEverythingWorksGeneric<bool>(true, dbType);
                this.TestBigIntForEverythingWorksGeneric<sbyte>((sbyte)1, dbType);
                this.TestBigIntForEverythingWorksGeneric<byte>((byte)1, dbType);
                this.TestBigIntForEverythingWorksGeneric<int>((int)1, dbType);
                this.TestBigIntForEverythingWorksGeneric<uint>((uint)1, dbType);
                this.TestBigIntForEverythingWorksGeneric<short>((short)1, dbType);
                this.TestBigIntForEverythingWorksGeneric<ushort>((ushort)1, dbType);
                this.TestBigIntForEverythingWorksGeneric<long>((long)1, dbType);
                this.TestBigIntForEverythingWorksGeneric<ulong>((ulong)1, dbType);
                this.TestBigIntForEverythingWorksGeneric<float>((float)1, dbType);
                this.TestBigIntForEverythingWorksGeneric<double>((double)1, dbType);
                this.TestBigIntForEverythingWorksGeneric<decimal>((decimal)1, dbType);

                this.TestBigIntForEverythingWorksGeneric(LotsOfNumerics.E_Byte.B, dbType);
                this.TestBigIntForEverythingWorksGeneric(LotsOfNumerics.E_SByte.B, dbType);
                this.TestBigIntForEverythingWorksGeneric(LotsOfNumerics.E_Int.B, dbType);
                this.TestBigIntForEverythingWorksGeneric(LotsOfNumerics.E_UInt.B, dbType);
                this.TestBigIntForEverythingWorksGeneric(LotsOfNumerics.E_Short.B, dbType);
                this.TestBigIntForEverythingWorksGeneric(LotsOfNumerics.E_UShort.B, dbType);
                this.TestBigIntForEverythingWorksGeneric(LotsOfNumerics.E_Long.B, dbType);
                this.TestBigIntForEverythingWorksGeneric(LotsOfNumerics.E_ULong.B, dbType);

                this.TestBigIntForEverythingWorksGeneric<bool?>(true, dbType);
                this.TestBigIntForEverythingWorksGeneric<sbyte?>((sbyte)1, dbType);
                this.TestBigIntForEverythingWorksGeneric<byte?>((byte)1, dbType);
                this.TestBigIntForEverythingWorksGeneric<int?>((int)1, dbType);
                this.TestBigIntForEverythingWorksGeneric<uint?>((uint)1, dbType);
                this.TestBigIntForEverythingWorksGeneric<short?>((short)1, dbType);
                this.TestBigIntForEverythingWorksGeneric<ushort?>((ushort)1, dbType);
                this.TestBigIntForEverythingWorksGeneric<long?>((long)1, dbType);
                this.TestBigIntForEverythingWorksGeneric<ulong?>((ulong)1, dbType);
                this.TestBigIntForEverythingWorksGeneric<float?>((float)1, dbType);
                this.TestBigIntForEverythingWorksGeneric<double?>((double)1, dbType);
                this.TestBigIntForEverythingWorksGeneric<decimal?>((decimal)1, dbType);

                this.TestBigIntForEverythingWorksGeneric<LotsOfNumerics.E_Byte?>(LotsOfNumerics.E_Byte.B, dbType);
                this.TestBigIntForEverythingWorksGeneric<LotsOfNumerics.E_SByte?>(LotsOfNumerics.E_SByte.B, dbType);
                this.TestBigIntForEverythingWorksGeneric<LotsOfNumerics.E_Int?>(LotsOfNumerics.E_Int.B, dbType);
                this.TestBigIntForEverythingWorksGeneric<LotsOfNumerics.E_UInt?>(LotsOfNumerics.E_UInt.B, dbType);
                this.TestBigIntForEverythingWorksGeneric<LotsOfNumerics.E_Short?>(LotsOfNumerics.E_Short.B, dbType);
                this.TestBigIntForEverythingWorksGeneric<LotsOfNumerics.E_UShort?>(LotsOfNumerics.E_UShort.B, dbType);
                this.TestBigIntForEverythingWorksGeneric<LotsOfNumerics.E_Long?>(LotsOfNumerics.E_Long.B, dbType);
                this.TestBigIntForEverythingWorksGeneric<LotsOfNumerics.E_ULong?>(LotsOfNumerics.E_ULong.B, dbType);
            }
        }

        private void TestBigIntForEverythingWorksGeneric<T>(T expected, string dbType)
        {
            using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
            {
                var query = database.Query<T>(FormattableStringFactory.Create("select cast(1 as " + dbType + ")")).Single();
                Assert.Equal(query, expected);

                var scalar = database.ExecuteScalar<T>(FormattableStringFactory.Create("select cast(1 as " + dbType + ")"));
                Assert.Equal(scalar, expected);
            }
        }

        [Fact]
        public void TestSubsequentQueriesSuccess()
        {
            using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
            {
                var data0 = database.Query<Fooz0>($"select 1 as [Id] where 1 = 0").ToList();
                Assert.Empty(data0);

                var data1 = database.Query<Fooz1>($"select 1 as [Id] where 1 = 0").ToList();
                Assert.Empty(data1);

                data0 = database.Query<Fooz0>($"select 1 as [Id] where 1 = 0").ToList();
                Assert.Empty(data0);

                data1 = database.Query<Fooz1>($"select 1 as [Id] where 1 = 0").ToList();
                Assert.Empty(data1);
            }
        }

        private class Fooz0
        {
            public int Id { get; set; }
        }

        private class Fooz1
        {
            public int Id { get; set; }
        }

        private class Fooz2
        {
            public int Id { get; set; }
        }

        private class RatingValueConverter : DbTypeConverter<RatingValue>
        {
            private RatingValueConverter()
            {
            }

            public static readonly RatingValueConverter Default = new RatingValueConverter();

            public override RatingValue Parse(object value)
            {
                if (value is int)
                {
                    return new RatingValue() { Value = (int)value };
                }

                throw new FormatException("Invalid conversion to RatingValue");
            }

            public override void SetValue(IDbDataParameter parameter, RatingValue value)
            {
                // ... null, range checks etc ...
                parameter.DbType = System.Data.DbType.Int32;
                parameter.Value = value.Value;
            }
        }

        public class RatingValue
        {
            public int Value { get; set; }
            // ... some other properties etc ...
        }

        public class MyResult
        {
            public string CategoryName { get; set; }
            public RatingValue CategoryRating { get; set; }
        }

        [Fact]
        public void SO24740733_TestCustomValueHandler()
        {
            using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
            {
                TypeProvider.AddTypeHandler(RatingValueConverter.Default);
                var foo = database.Query<MyResult>($"SELECT 'Foo' AS CategoryName, 200 AS CategoryRating").Single();

                Assert.Equal("Foo", foo.CategoryName);
                Assert.Equal(200, foo.CategoryRating.Value);
            }
        }

        [Fact]
        public void SO24740733_TestCustomValueSingleColumn()
        {
            using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
            {
                TypeProvider.AddTypeHandler(RatingValueConverter.Default);
                var foo = database.Query<RatingValue>($"SELECT 200 AS CategoryRating").Single();

                Assert.Equal(200, foo.Value);
            }
        }

        private class StringListDbTypeConverter : DbTypeConverter<List<string>>
        {
            private StringListDbTypeConverter()
            {
            }

            public static readonly StringListDbTypeConverter Default = new StringListDbTypeConverter();
            //Just a simple List<string> type handler implementation
            public override void SetValue(IDbDataParameter parameter, List<string> value)
            {
                parameter.Value = string.Join(",", value);
            }

            public override List<string> Parse(object value)
            {
                return ((value as string) ?? "").Split(',').ToList();
            }
        }

        public class MyObjectWithStringList
        {
            public List<string> Names { get; set; }
        }

        [Fact]
        public void Issue253_TestIEnumerableTypeHandlerParsing()
        {
            using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
            {
                TypeProvider.ResetTypeHandlers();
                TypeProvider.AddTypeHandler(StringListDbTypeConverter.Default);
                var foo = database.Query<MyObjectWithStringList>($"SELECT 'Sam,Kyro' AS Names").Single();
                Assert.Equal(new[] { "Sam", "Kyro" }, foo.Names);
            }
        }

        [Fact(Skip = "Not working")]
        public void Issue253_TestIEnumerableTypeHandlerSetParameterValue()
        {
            using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
            {
                TypeProvider.ResetTypeHandlers();
                TypeProvider.AddTypeHandler(StringListDbTypeConverter.Default);

                database.Execute($"CREATE TABLE #Issue253 (Names VARCHAR(50) NOT NULL);");
                try
                {
                    const string names = "Sam,Kyro";
                    List<string> names_list = names.Split(',').ToList();
                    var foo = database.Query<string>($"INSERT INTO #Issue253 (Names) VALUES ({names_list}); SELECT Names FROM #Issue253;").Single();
                    Assert.Equal(foo, names);
                }
                finally
                {
                    database.Execute($"DROP TABLE #Issue253;");
                }
            }
        }

        private class RecordingDbTypeConverter<T>
            : DbTypeConverter<T>
        {
            public override void SetValue(IDbDataParameter parameter, T value)
            {
                this.SetValueWasCalled = true;
                parameter.Value = value;
            }

            public override T Parse(object value)
            {
                this.ParseWasCalled = true;
                return (T)value;
            }

            public bool SetValueWasCalled { get; set; }
            public bool ParseWasCalled { get; set; }
        }

        [Fact(Skip = "Not working")]
        public void Test_RemoveTypeMap()
        {
            using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
            {
                TypeProvider.ResetTypeHandlers();
                TypeProvider.RemoveTypeMap(typeof(DateTime));

                var dateTimeHandler = new RecordingDbTypeConverter<DateTime>();
                TypeProvider.AddTypeHandler(dateTimeHandler);

                database.Execute($"CREATE TABLE #Test_RemoveTypeMap (x datetime NOT NULL);");

                try
                {
                    database.Execute($"INSERT INTO #Test_RemoveTypeMap VALUES ({DateTime.Now})");
                    database.Query<DateTime>($"SELECT * FROM #Test_RemoveTypeMap");

                    Assert.True(dateTimeHandler.ParseWasCalled);
                    Assert.True(dateTimeHandler.SetValueWasCalled);
                }
                finally
                {
                    database.Execute($"DROP TABLE #Test_RemoveTypeMap");
                    TypeProvider.AddTypeMap(typeof(DateTime), DbType.DateTime); // or an option to reset type map?
                }
            }
        }

        [Fact]
        public void TestReaderWhenResultsChange()
        {
            using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
            {
                try
                {
                    database.Execute(
                        $"create table #ResultsChange (X int);create table #ResultsChange2 (Y int);insert #ResultsChange (X) values(1);insert #ResultsChange2 (Y) values(1);");

                    var obj1 = database.Query<ResultsChangeType>($"select * from #ResultsChange").Single();
                    Assert.Equal(1, obj1.X);
                    Assert.Equal(0, obj1.Y);
                    Assert.Equal(0, obj1.Z);

                    var obj2 = database.Query<ResultsChangeType>($"select * from #ResultsChange rc inner join #ResultsChange2 rc2 on rc2.Y=rc.X")
                                   .Single();
                    Assert.Equal(1, obj2.X);
                    Assert.Equal(1, obj2.Y);
                    Assert.Equal(0, obj2.Z);

                    database.Execute($"alter table #ResultsChange add Z int null");
                    database.Execute($"update #ResultsChange set Z = 2");

                    var obj3 = database.Query<ResultsChangeType>($"select * from #ResultsChange").Single();
                    Assert.Equal(1, obj3.X);
                    Assert.Equal(0, obj3.Y);
                    Assert.Equal(2, obj3.Z);

                    var obj4 = database.Query<ResultsChangeType>($"select * from #ResultsChange rc inner join #ResultsChange2 rc2 on rc2.Y=rc.X")
                                   .Single();
                    Assert.Equal(1, obj4.X);
                    Assert.Equal(1, obj4.Y);
                    Assert.Equal(2, obj4.Z);
                }
                finally
                {
                    database.Execute($"drop table #ResultsChange;drop table #ResultsChange2;");
                }
            }
        }

        private class ResultsChangeType
        {
            public int X { get; set; }
            public int Y { get; set; }
            public int Z { get; set; }
        }

        public class WrongTypes
        {
            public int A { get; set; }
            public double B { get; set; }
            public long C { get; set; }
            public bool D { get; set; }
        }

        [Fact]
        public void TestWrongTypes_WithRightTypes()
        {
            using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
            {
                var item = database.Query<WrongTypes>($"select 1 as A, cast(2.0 as float) as B, cast(3 as bigint) as C, cast(1 as bit) as D").Single();
                item.A.Equals(1);
                item.B.Equals(2.0);
                item.C.Equals(3L);
                item.D.Equals(true);
            }
        }

        [Fact]
        public void TestWrongTypes_WithWrongTypes()
        {
            using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
            {
                var item = database.Query<WrongTypes>($"select cast(1.0 as float) as A, 2 as B, 3 as C, cast(1 as bigint) as D").Single();
                item.A.Equals(1);
                item.B.Equals(2.0);
                item.C.Equals(3L);
                item.D.Equals(true);
            }
        }

        [Fact]
        public void SO24607639_NullableBools()
        {
            using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
            {
                var obj = database.Query<HazBools>(
                    $@"declare @vals table (A bit null, B bit null, C bit null);
                insert @vals (A,B,C) values (1,0,null);
                select * from @vals").Single();
                Assert.NotNull(obj);
                Assert.True(obj.A.Value);
                Assert.False(obj.B.Value);
                Assert.Null(obj.C);
            }
        }

        private class HazBools
        {
            public bool? A { get; set; }
            public bool? B { get; set; }
            public bool? C { get; set; }
        }

        [Fact]
        public void Issue149_TypeMismatch_SequentialAccess()
        {
            using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
            {
                Guid guid = Guid.Parse("cf0ef7ac-b6fe-4e24-aeda-a2b45bb5654e");
                var ex = Assert.ThrowsAny<Exception>(() => database.Query<Issue149_Person>($"select {guid} as Id").First());
                Assert.Equal("Error parsing column 0 (Id=cf0ef7ac-b6fe-4e24-aeda-a2b45bb5654e - Object)", ex.Message);
            }
        }

        public class Issue149_Person { public string Id { get; set; } }

        [Fact]
        public void Issue295_NullableDateTime_SqlServer()
        {
            using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
            {
                Common.TestDateTime(database);
            }
        }

        [Fact]
        public void SO29343103_UtcDates()
        {
            using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
            {
                var date = DateTime.UtcNow;
                var returned = database.Query<DateTime>($"select {date}").Single();
                var delta = returned - date;
                Assert.True(delta.TotalMilliseconds >= -10 && delta.TotalMilliseconds <= 10);
            }
        }

        [Fact(Skip = "Not working")]
        public void Issue461_TypeHandlerWorksInConstructor()
        {
            using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
            {
                TypeProvider.AddTypeHandler(new Issue461BlargConverter());

                database.Execute($@"CREATE TABLE #Issue461 (
                                      Id                int not null IDENTITY(1,1),
                                      SomeValue         nvarchar(50),
                                      SomeBlargValue    nvarchar(200),
                                    )");
                const string Expected = "abc123def";
                var blarg = new Blarg(Expected);
                database.Execute(
                    $"INSERT INTO #Issue461 (SomeValue, SomeBlargValue) VALUES ({"what up?"}, {blarg})");

                // test: without constructor
                var parameterlessWorks = database.QuerySingle<Issue461_ParameterlessTypeConstructor>($"SELECT * FROM #Issue461");
                Assert.Equal(1, parameterlessWorks.Id);
                Assert.Equal("what up?", parameterlessWorks.SomeValue);
                Assert.Equal(parameterlessWorks.SomeBlargValue.Value, Expected);

                // test: via constructor
                var parameterDoesNot = database.QuerySingle<Issue461_ParameterisedTypeConstructor>($"SELECT * FROM #Issue461");
                Assert.Equal(1, parameterDoesNot.Id);
                Assert.Equal("what up?", parameterDoesNot.SomeValue);
                Assert.Equal(parameterDoesNot.SomeBlargValue.Value, Expected);
            }
        }

        // I would usually expect this to be a struct; using a class
        // so that we can't pass unexpectedly due to forcing an unsafe cast - want
        // to see an InvalidCastException if it is wrong
        private class Blarg
        {
            public Blarg(string value) { this.Value = value; }
            public string Value { get; }
            public override string ToString()
            {
                return this.Value;
            }
        }

        private class Issue461BlargConverter : DbTypeConverter<Blarg>
        {
            public override void SetValue(IDbDataParameter parameter, Blarg value)
            {
                parameter.Value = ((object)value.Value) ?? DBNull.Value;
            }

            public override Blarg Parse(object value)
            {
                string s = (value == null || value is DBNull) ? null : Convert.ToString(value);
                return new Blarg(s);
            }
        }

        private class Issue461_ParameterlessTypeConstructor
        {
            public int Id { get; set; }

            public string SomeValue { get; set; }
            public Blarg SomeBlargValue { get; set; }
        }

        private class Issue461_ParameterisedTypeConstructor
        {
            public Issue461_ParameterisedTypeConstructor(int id, string someValue, Blarg someBlargValue)
            {
                this.Id = id;
                this.SomeValue = someValue;
                this.SomeBlargValue = someBlargValue;
            }

            public int Id { get; }

            public string SomeValue { get; }
            public Blarg SomeBlargValue { get; }
        }
    }
}
