namespace PeregrineDb.Tests.Databases
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Data.SqlClient;
    using System.Diagnostics;
    using System.Diagnostics.CodeAnalysis;
    using System.Dynamic;
    using System.Globalization;
    using System.Linq;
    using Dapper;
    using FluentAssertions;
    using PeregrineDb.Tests.ExampleEntities;
    using PeregrineDb.Tests.SharedTypes;
    using PeregrineDb.Tests.Testing;
    using PeregrineDb.Tests.Utils;
    using Xunit;

    public abstract partial class DefaultDatabaseConnectionStatementsTests
    {
        [SuppressMessage("ReSharper", "AccessToDisposedClosure")]
        public abstract class Query
            : DefaultDatabaseConnectionStatementsTests
        {
            public class Constructors
                : Query
            {
                [Fact]
                public void Uses_public_parameterless_constructor()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        // Act
                        var result = database.Query<PublicParameterlessConstructor>("select 1 A").First();

                        // Assert
                        result.A.Should().Be(1);
                    }
                }

                [Fact]
                public void Ignores_non_parameterless_constructor()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        // Act
                        var result = database.Query<MultipleConstructors>("select 1 A").First();

                        // Assert
                        result.A.Should().Be(1);
                    }
                }

                [Fact(Skip = "Not implemented")]
                public void Errors_when_no_public_constructor_is_found()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        // Act
                        Action act = () => database.Query<NoPublicConstructor>("select 1 as A");

                        // Assert
                        act.ShouldThrow<InvalidOperationException>()
                           .WithMessage("PeregrineDb.Tests.SharedTypes.NoPublicConstructor must have a public parameterless constructor");
                    }
                }

                [Fact(Skip = "Not implemented")]
                public void Errors_when_no_parameterless_constructor_is_found()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        // Act
                        Action act = () => database.Query<NoParameterlessConstructor>("select 1 as A");

                        // Assert
                        act.ShouldThrow<InvalidOperationException>()
                           .WithMessage("PeregrineDb.Tests.SharedTypes.NoParameterlessConstructor must have a public parameterless constructor");
                    }
                }
            }

            public class Inheritance
            {
                [Fact]
                public void TestInheritance()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        // Act
                        var result = database.Query<InheritanceTest2>("select \'One\' as Derived1, \'Two\' as Base1").First();

                        // Assert
                        Assert.Equal("One", result.Derived1);
                        Assert.Equal("Two", result.Base1);
                    }
                }

                [Fact]
                public void Sets_property_inherited_from_base_class()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        // Act
                        var order = database.Query<AbstractInheritance.ConcreteOrder>("select 3 [Public], 4 Concrete").First();

                        // Assert
                        Assert.Equal(3, order.Public);
                        Assert.Equal(4, order.Concrete);
                    }
                }
            }

            public class Misc
                : Query
            {
                [Fact]
                public void Returns_empty_set_when_sql_is_not_a_query()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        // Act / Assert
                        database.Query<int?>("print \'not a query\'").Should().BeEmpty();
                        database.Query<GenericEntity<int?>>("print \'not a query\'").Should().BeEmpty();
                    }
                }

                [Fact]
                public void Returns_empty_set_when_query_returns_no_rows()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        // Act / Assert
                        database.Query<int?>("SELECT 5 WHERE 1 = 0").Should().BeEmpty();
                    }
                }

                [FactLongRunning]
                public void Allows_for_a_long_running_query()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        // Act
                        var watch = Stopwatch.StartNew();
                        var i = database.Query<int>("waitfor delay \'00:01:00\'; select 42;", 300).Single();
                        watch.Stop();

                        // Assert
                        i.Should().Be(42);
                        watch.Elapsed.TotalMinutes.Should().BeGreaterOrEqualTo(0.95).And.BeLessOrEqualTo(1.05);
                    }
                }

                [Fact]
                public void Timesout_and_throws_error()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        // Act
                        Action act = () => database.Query<int>("waitfor delay \'00:01:00\'; select 42;", 1);

                        // Assert
                        act.ShouldThrow<SqlException>().And.Message.Should().Contain("Timeout expired");
                    }
                }

                [Fact(Skip = "Not implemented")]
                public void Does_not_set_internal_properties()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        // Act / Assert
                        Assert.Equal(default, database.Query<TestObj>("select 10 as [Internal]").First()._internal);
                    }
                }

                [Fact(Skip = "Not implemented")]
                public void Does_not_set_private_properties()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        // Act / Assert
                        Assert.Equal(default, database.Query<TestObj>("select 10 as [Priv]").First()._priv);
                    }
                }

                [Fact(Skip = "Not implemented")]
                public void Does_not_populate_fields()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        // Act
                        var data = database.Query<TestFieldsEntity>("select a=1,b=2,c=3").Single();

                        // Assert
                        Assert.Equal(default, data.a);
                        Assert.Equal(5, data.b);
                        Assert.Equal(default, data.GetC());
                    }
                }

                /// <summary>
                /// This test makes sure that any caches are invalidated properly if schema changes.
                /// </summary>
                [Fact]
                public void Allows_schema_to_change_between_consecutive_queries()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        // Arrange
                        database.Execute("create table #dog(Age int, Name nvarchar(max)) insert #dog values(1, \'Alf\')");

                        var beforeResult = database.Query<Dog>("select * from #dog").Single();
                        Assert.Equal("Alf", beforeResult.Name);
                        Assert.Equal(1, beforeResult.Age);

                        database.Execute("alter table #dog drop column Name");

                        // Act
                        var afterResult = database.Query<Dog>("select * from #dog").Single();

                        // Assert
                        Assert.Null(afterResult.Name);
                        Assert.Equal(1, afterResult.Age);
                    }
                }

                [Fact]
                public void Ignores_extra_returned_fields()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        // Arrange
                        var id = 5;

                        // Act
                        var dog = database.Query<Dog>("select '' as Extra, 1 as Age, 'Rover' as Name, Id = @value", new { value = id });

                        // Assert
                        dog.ShouldAllBeEquivalentTo(new[]
                            {
                                new Dog { Id = id, Age = 1, Name = "Rover" }
                            });
                    }
                }
            }

            public class Parameters
                : Query
            {
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

                            database.Query<int>("select @pid", new { PId = 1 }).ShouldAllBeEquivalentTo(new[] { 1 });
                        }
                        finally
                        {
                            CultureInfo.CurrentCulture = current;
                        }
                    }
                }

                [Fact]
                public void Does_not_allow_an_enumerable_parameter()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        // Act
                        var parameters = new[]
                            {
                                new { A = "A", B = "B" }
                            };
                        Action act = () => database.Query<dynamic>("Select @A, @B", parameters);

                        // Assert
                        act.ShouldThrow<InvalidOperationException>().WithMessage("An enumerable sequence of parameters (arrays, lists, etc) is not allowed in this context");
                    }
                }

                [Fact]
                public void Ignores_indexer_properties()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        // Act
                        var result = database.Query<ParameterWithIndexer>("SELECT @A as A", new ParameterWithIndexer { A = 5 }).Single();

                        // Assert
                        result.A.Should().Be(5);
                    }
                }
            }

            public class Strings
                : Query
            {
                [Fact]
                public void Can_query_strings()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        database.Query<string>("select \'a\' a union select \'b\'")
                                .ShouldAllBeEquivalentTo(new[] { "a", "b" }, o => o.WithStrictOrdering());

                        database.Query<GenericEntity<string>>("select \'a\' Value union select \'b\'")
                                .Select(e => e.Value)
                                .ShouldAllBeEquivalentTo(new[] { "a", "b" }, o => o.WithStrictOrdering());
                    }
                }

                [Fact]
                public void Can_query_null_string()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        database.Query<string>("select CAST (null as nvarchar(MAX))")
                                .ShouldAllBeEquivalentTo(new[] { (string)null }, o => o.WithStrictOrdering());

                        database.Query<GenericEntity<string>>("select CAST (null as nvarchar(MAX))")
                                .Select(e => e.Value)
                                .ShouldAllBeEquivalentTo(new[] { (string)null }, o => o.WithStrictOrdering());
                    }
                }

                [Fact]
                public void Can_query_naked_empty_string()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        database.Query<string>("select \'\'")
                                .ShouldAllBeEquivalentTo(new[] { string.Empty }, o => o.WithStrictOrdering());

                        database.Query<GenericEntity<string>>("select \'\' Value")
                                .Select(e => e.Value)
                                .ShouldAllBeEquivalentTo(new[] { string.Empty }, o => o.WithStrictOrdering());
                    }
                }

                [Fact]
                public void Parameter_can_have_a_string()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.PostgreSql))
                    {
                        database.Query<string>("select @Value", new { value = "a" }).ShouldAllBeEquivalentTo(new[] { "a" });
                    }
                }

                [Fact]
                public void Parameter_can_have_a_null_string()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.PostgreSql))
                    {
                        database.Query<string>("select @Value", new { value = (string)null }).ShouldAllBeEquivalentTo(new[] { (string)null });
                    }
                }

                [Fact]
                public void Parameter_can_have_a_list_of_strings()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.PostgreSql))
                    {
                        var values = new[] { "a", "b", "c" }.ToList();
                        database.Query<string>("select * from (select 'a' as x union all select 'b' union all select 'c') as T where x = ANY (@values)", new { values })
                                .ShouldAllBeEquivalentTo(values, o => o.WithStrictOrdering());

                        var emptyList = new string[0].ToList();
                        database.Query<string>("select * from (select 'a' as x union all select 'b' union all select 'c') as T where x = ANY (@values)", new { values = emptyList })
                                .Should().BeEmpty();
                    }
                }

                [Fact]
                public void Parameter_can_have_massive_string()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        var str = new string('X', 20000);
                        database.Query<string>("select @value", new { value = str }).ShouldAllBeEquivalentTo(new[] { str });
                    }
                }

                [Fact(Skip = "Not implemented")]
                public void TestChangingDefaultStringTypeMappingToAnsiString()
                {
                    ////using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    ////{
                    ////    var result01 = database.Query<string>("SELECT SQL_VARIANT_PROPERTY(CONVERT(sql_variant, @value),'BaseType') AS BaseType", new { value = "TestString" }).FirstOrDefault();
                    ////    Assert.Equal("nvarchar", result01);

                    ////    QueryCache.Purge();

                    ////    TypeProvider.AddTypeMap(typeof(string), DbType.AnsiString); // Change Default String Handling to AnsiString
                    ////    var result02 = database.Query<string>("SELECT SQL_VARIANT_PROPERTY(CONVERT(sql_variant, @value),'BaseType') AS BaseType", new { value = "TestString" }).FirstOrDefault();
                    ////    Assert.Equal("varchar", result02);

                    ////    QueryCache.Purge();
                    ////    TypeProvider.AddTypeMap(typeof(string), DbType.String); // Restore Default to Unicode String
                    ////}
                }

                [Fact(Skip = "Not implemented")]
                public void TestChangingDefaultStringTypeMappingToAnsiStringFirstOrDefault()
                {
                    ////using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    ////{
                    ////    var result01 = database.QueryFirstOrDefault<string>("SELECT SQL_VARIANT_PROPERTY(CONVERT(sql_variant, @value),'BaseType') AS BaseType", new { value = "TestString" });
                    ////    Assert.Equal("nvarchar", result01);

                    ////    QueryCache.Purge();

                    ////    TypeProvider.AddTypeMap(typeof(string), DbType.AnsiString); // Change Default String Handling to AnsiString
                    ////    var result02 = database.QueryFirstOrDefault<string>("SELECT SQL_VARIANT_PROPERTY(CONVERT(sql_variant, @value),'BaseType') AS BaseType", new { value = "TestString" });
                    ////    Assert.Equal("varchar", result02);

                    ////    QueryCache.Purge();
                    ////    TypeProvider.AddTypeMap(typeof(string), DbType.String); // Restore Default to Unicode String
                    ////}
                }
            }

            /// <summary>
            /// TODO: Make DbString use typeconverter
            /// </summary>
            public class DbStrings
                : Query
            {
                [Fact]
                public void TestDbString()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        database.Query<int>("select datalength(@value)", new { value = new DbString { Value = "abc", IsAnsi = true } }).Single().Should().Be(3);
                        database.Query<int>("select datalength(@value)", new { value = new DbString { Value = "abc", IsAnsi = false } }).Single().Should().Be(6);
                        database.Query<int>("select datalength(@value)", new { value = new DbString { Value = "abc", IsFixedLength = true, Length = 10, IsAnsi = true } }).Single().Should().Be(10);
                        database.Query<int>("select datalength(@value)", new { value = new DbString { Value = "abc", IsFixedLength = true, Length = 10, IsAnsi = false } }).Single().Should().Be(20);
                        database.Query<int>("select datalength(@value)", new { value = new DbString { Value = "abc", IsFixedLength = false, Length = 10, IsAnsi = true } }).Single().Should().Be(3);
                        database.Query<int>("select datalength(@value)", new { value = new DbString { Value = "abc", IsFixedLength = false, Length = 10, IsAnsi = false } }).Single().Should().Be(6);
                    }
                }
            }

            public class Characters
                : Query
            {
                [Fact]
                public void Can_use_chars()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        const char value = '〠';

                        database.Query<char>("select @value", new { value }).ShouldAllBeEquivalentTo(new[] { value });
                        database.Query<char?>("select @value", new { value = (char?)value }).ShouldAllBeEquivalentTo(new[] { (char?)value });
                        database.Query<char?>("select @value", new { value = (char?)null }).ShouldAllBeEquivalentTo(new[] { (char?)null });

                        database.Query<GenericEntity<char>>("select @value AS Value", new { value }).Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { value });
                        database.Query<GenericEntity<char?>>("select @value AS Value", new { value = (char?)value }).Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { (char?)value });
                        database.Query<GenericEntity<char?>>("select @value AS Value", new { value = (char?)null }).Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { (char?)null });
                    }
                }

                [Fact]
                public void Can_read_single_character_strings()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        database.Query<char>("select \'f\'").ShouldAllBeEquivalentTo(new[] { 'f' });
                        database.Query<GenericEntity<char>>("select \'f\' as Value").Select(c => c.Value).ShouldAllBeEquivalentTo(new[] { 'f' });

                        database.Query<char?>("select \'f\'").ShouldAllBeEquivalentTo(new[] { 'f' });
                        database.Query<GenericEntity<char?>>("select \'f\' as Value").Select(c => c.Value).ShouldAllBeEquivalentTo(new[] { 'f' });
                    }
                }

                [Fact]
                public void Throws_exception_if_string_has_more_than_one_character()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        Action act = () => database.Query<char>("select \'foo\'");
                        act.ShouldThrow<ArgumentException>().WithMessage("A single-character was expected\r\n\r\nParameter name: value");
                    }
                }
            }

            public class Integers
                : Query
            {
                [Fact]
                public void Can_use_int16()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        var value = (short)42;

                        database.Query<short>("select @value", new { value }).ShouldAllBeEquivalentTo(new[] { value });
                        database.Query<short?>("select @value", new { value = (short?)value }).ShouldAllBeEquivalentTo(new[] { (short?)value });
                        database.Query<short?>("select @value", new { value = (short?)null }).ShouldAllBeEquivalentTo(new[] { (short?)null });

                        database.Query<GenericEntity<short>>("select @value AS Value", new { value }).Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { value });
                        database.Query<GenericEntity<short?>>("select @value AS Value", new { value = (short?)value }).Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { (short?)value });
                        database.Query<GenericEntity<short?>>("select @value AS Value", new { value = (short?)null }).Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { (short?)null });
                    }
                }

                [Fact]
                public void Can_use_int32()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        var value = 42;

                        database.Query<int>("select @value", new { value }).ShouldAllBeEquivalentTo(new[] { value });
                        database.Query<int?>("select @value", new { value = (int?)value }).ShouldAllBeEquivalentTo(new[] { (long?)value });
                        database.Query<int?>("select @value", new { value = (int?)null }).ShouldAllBeEquivalentTo(new[] { (long?)null });

                        database.Query<GenericEntity<int>>("select @value AS Value", new { value }).Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { value });
                        database.Query<GenericEntity<int?>>("select @value AS Value", new { value = (int?)value }).Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { (int?)value });
                        database.Query<GenericEntity<int?>>("select @value AS Value", new { value = (int?)null }).Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { (int?)null });
                    }
                }

                [Fact]
                public void Can_read_list_of_int32()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        database.Query<int>("select 1 union all select 2 union all select 3")
                                .ShouldAllBeEquivalentTo(new[] { 1, 2, 3 }, o => o.WithStrictOrdering());
                    }
                }

                [Fact]
                public void Can_use_int64()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        var value = 42L;

                        database.Query<long>("select @value", new { value }).ShouldAllBeEquivalentTo(new[] { value });
                        database.Query<long?>("select @value", new { value = (long?)value }).ShouldAllBeEquivalentTo(new[] { (long?)value });
                        database.Query<long?>("select @value", new { value = (long?)null }).ShouldAllBeEquivalentTo(new[] { (long?)null });

                        database.Query<GenericEntity<long>>("select @value AS Value", new { value }).Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { value });
                        database.Query<GenericEntity<long?>>("select @value AS Value", new { value = (long?)value }).Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { (long?)value });
                        database.Query<GenericEntity<long?>>("select @value AS Value", new { value = (long?)null }).Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { (long?)null });
                    }
                }

                [Fact]
                public void Converts_int64_to_int32()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        database.Query<int>("select cast(42 as bigint) as Value").ShouldAllBeEquivalentTo(new[] { 42 });
                        database.Query<GenericEntity<int>>("select cast(42 as bigint) as Value").Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { 42 });
                    }
                }

                [Fact]
                public void Throws_error_if_int64_downward_conversion_is_out_of_range()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        Action act = () => database.Query<int>("select @value", new { value = (long)int.MaxValue + 2 });
                        act.ShouldThrow<OverflowException>().WithMessage("Value was either too large or too small for an Int32.");

                        act = () => database.Query<GenericEntity<int>>("select cast(@value as bigint) as Value", new { value = (long)int.MaxValue + 2 });
                        act.ShouldThrow<Exception>().WithMessage("Error parsing column 0 (Value=2147483649 - Int64)");
                    }
                }

                [Fact]
                public void Parameter_can_have_list_of_int32()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.PostgreSql))
                    {
                        var values = new[] { 1, 2, 3 }.AsEnumerable();
                        database.Query<int>("select * from (select 1 as Id union all select 2 union all select 3) as X where Id = ANY (@values)", new { values })
                                .ShouldAllBeEquivalentTo(new[] { 1, 2, 3 }, o => o.WithStrictOrdering());
                    }
                }

                [Fact]
                public void Parameter_can_have_empty_list_of_int32()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.PostgreSql))
                    {
                        var values = new int[0];
                        database.Query<int>("select * from (select 1 as Id union all select 2 union all select 3) as X where Id = ANY (@values)", new { values })
                                .Should().BeEmpty();
                    }
                }
            }

            public class Doubles
                : Query
            {
                [Fact]
                public void Can_use_doubles()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        var value = 0.1d;

                        database.Query<double>("select @value", new { value }).ShouldAllBeEquivalentTo(new[] { value });
                        database.Query<double?>("select @value", new { value = (double?)value }).ShouldAllBeEquivalentTo(new[] { (double?)value });
                        database.Query<double?>("select @value", new { value = (double?)null }).ShouldAllBeEquivalentTo(new[] { (double?)null });

                        database.Query<GenericEntity<double>>("select @value AS Value", new { value }).Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { value });
                        database.Query<GenericEntity<double?>>("select @value AS Value", new { value = (double?)value }).Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { (double?)value });
                        database.Query<GenericEntity<double?>>("select @value AS Value", new { value = (double?)null }).Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { (double?)null });
                    }
                }
            }

            public class Decimals
                : Query
            {
                [Fact]
                public void Can_use_decimals()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        var value = 11.884M;

                        database.Query<decimal>("select @value", new { value }).ShouldAllBeEquivalentTo(new[] { value });
                        database.Query<decimal?>("select @value", new { value = (decimal?)value }).ShouldAllBeEquivalentTo(new[] { (decimal?)value });
                        database.Query<decimal?>("select @value", new { value = (decimal?)null }).ShouldAllBeEquivalentTo(new[] { (decimal?)null });

                        database.Query<GenericEntity<decimal>>("select @value AS Value", new { value }).Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { value });
                        database.Query<GenericEntity<decimal?>>("select @value AS Value", new { value = (decimal?)value }).Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { (decimal?)value });
                        database.Query<GenericEntity<decimal?>>("select @value AS Value", new { value = (decimal?)null }).Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { (decimal?)null });
                    }
                }

                [Fact]
                public void Converts_doubles_to_decimals()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        database.Query<decimal>("select cast(1 as float)").ShouldAllBeEquivalentTo(new[] { 1.0M });
                        database.Query<GenericEntity<decimal>>("select cast(1 as float) as Value").Select(e => e.Value)
                                .ShouldAllBeEquivalentTo(new[] { 1.0M });
                    }
                }

                /// <summary>
                /// TODO: Why the inconsistency?
                /// </summary>
                [Fact]
                public void Converts_null_doubles_to_default_decimals()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        database.Query<decimal?>("select cast(null as float)").ShouldAllBeEquivalentTo(new[] { (decimal?)null });
                        database.Query<GenericEntity<decimal>>("select cast(null as float) as Value").Select(e => e.Value)
                                .ShouldAllBeEquivalentTo(new[] { 0.0M });
                    }
                }
            }

            public class Booleans
                : Query
            {
                [Fact]
                public void Can_use_booleans()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        database.Query<bool>("select @value", new { value = true }).ShouldAllBeEquivalentTo(new[] { true });
                        database.Query<bool>("select @value", new { value = false }).ShouldAllBeEquivalentTo(new[] { false });
                        database.Query<bool?>("select @value", new { value = (bool?)true }).ShouldAllBeEquivalentTo(new[] { (bool?)true });
                        database.Query<bool?>("select @value", new { value = (bool?)false }).ShouldAllBeEquivalentTo(new[] { (bool?)false });
                        database.Query<bool?>("select @value", new { value = (bool?)null }).ShouldAllBeEquivalentTo(new[] { (bool?)null });

                        database.Query<GenericEntity<bool>>("select @value AS Value", new { value = true }).Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { true });
                        database.Query<GenericEntity<bool>>("select @value AS Value", new { value = false }).Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { false });
                        database.Query<GenericEntity<bool?>>("select @value AS Value", new { value = (bool?)true }).Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { (bool?)true });
                        database.Query<GenericEntity<bool?>>("select @value AS Value", new { value = (bool?)false }).Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { (bool?)false });
                        database.Query<GenericEntity<bool?>>("select @value AS Value", new { value = (bool?)null }).Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { (bool?)null });
                    }
                }

                [Fact]
                public void Converts_sql_bit_to_boolean()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        database.Query<bool>("select CAST(1 as BIT)").ShouldAllBeEquivalentTo(new[] { true });
                        database.Query<bool>("select CAST(0 as BIT)").ShouldAllBeEquivalentTo(new[] { false });

                        database.Query<bool?>("select CAST(1 as BIT)").ShouldAllBeEquivalentTo(new[] { true });
                        database.Query<bool?>("select CAST(0 as BIT)").ShouldAllBeEquivalentTo(new[] { false });
                        database.Query<bool?>("select CAST(NULL as BIT)").ShouldAllBeEquivalentTo(new[] { (bool?)null });

                        database.Query<GenericEntity<bool>>("select CAST(1 as BIT) AS Value").Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { true });
                        database.Query<GenericEntity<bool>>("select CAST(0 as BIT) AS Value").Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { false });

                        database.Query<GenericEntity<bool?>>("select CAST(1 as BIT) AS Value").Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { true });
                        database.Query<GenericEntity<bool?>>("select CAST(0 as BIT) AS Value").Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { false });
                        database.Query<GenericEntity<bool?>>("select CAST(NULL as BIT) AS Value").Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { (bool?)null });
                    }
                }

                [Fact]
                public void Converts_sql_tinyint_to_boolean()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        database.Query<bool>("select CAST(1 as tinyint)").ShouldAllBeEquivalentTo(new[] { true });
                        database.Query<bool>("select CAST(0 as tinyint)").ShouldAllBeEquivalentTo(new[] { false });

                        database.Query<bool?>("select CAST(1 as tinyint)").ShouldAllBeEquivalentTo(new[] { true });
                        database.Query<bool?>("select CAST(0 as tinyint)").ShouldAllBeEquivalentTo(new[] { false });
                        database.Query<bool?>("select CAST(NULL as tinyint)").ShouldAllBeEquivalentTo(new[] { (bool?)null });

                        database.Query<GenericEntity<bool>>("select CAST(1 as tinyint) AS Value").Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { true });
                        database.Query<GenericEntity<bool>>("select CAST(0 as tinyint) AS Value").Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { false });

                        database.Query<GenericEntity<bool?>>("select CAST(1 as tinyint) AS Value").Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { true });
                        database.Query<GenericEntity<bool?>>("select CAST(0 as tinyint) AS Value").Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { false });
                        database.Query<GenericEntity<bool?>>("select CAST(NULL as tinyint) AS Value").Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { (bool?)null });
                    }
                }

                [Fact]
                public void Converts_pg_booleans_to_boolean()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.PostgreSql))
                    {
                        database.Query<bool>("select TRUE").ShouldAllBeEquivalentTo(new[] { true });
                        database.Query<bool>("select FALSE").ShouldAllBeEquivalentTo(new[] { false });

                        database.Query<bool?>("select TRUE").ShouldAllBeEquivalentTo(new[] { true });
                        database.Query<bool?>("select FALSE").ShouldAllBeEquivalentTo(new[] { false });
                        database.Query<bool?>("select NULL::boolean").ShouldAllBeEquivalentTo(new[] { (bool?)null });

                        database.Query<GenericEntity<bool>>("select TRUE AS Value").Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { true });
                        database.Query<GenericEntity<bool>>("select FALSE AS Value").Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { false });

                        database.Query<GenericEntity<bool?>>("select TRUE AS Value").Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { true });
                        database.Query<GenericEntity<bool?>>("select FALSE AS Value").Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { false });
                        database.Query<GenericEntity<bool?>>("select NULL::boolean AS Value").Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { (bool?)null });
                    }
                }
            }

            public class Guids
                : Query
            {
                [Fact]
                public void Can_use_guids()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        var value = Guid.NewGuid();

                        database.Query<Guid>("select @value", new { value }).ShouldAllBeEquivalentTo(new[] { value });
                        database.Query<Guid?>("select @value", new { value = (Guid?)value }).ShouldAllBeEquivalentTo(new[] { (Guid?)value });
                        database.Query<Guid?>("select @value", new { value = (Guid?)null }).ShouldAllBeEquivalentTo(new[] { (Guid?)null });

                        database.Query<GenericEntity<Guid>>("select @value AS Value", new { value }).Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { value });
                        database.Query<GenericEntity<Guid?>>("select @value AS Value", new { value = (Guid?)value }).Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { (Guid?)value });
                        database.Query<GenericEntity<Guid?>>("select @value AS Value", new { value = (Guid?)null }).Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { (Guid?)null });
                    }
                }

                [Fact]
                public void Does_not_auto_convert_strings_to_guids()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        // Act
                        Action act = () => database.Query<GenericEntity<string>>("select @value as Value", new { value = Guid.Parse("cf0ef7ac-b6fe-4e24-aeda-a2b45bb5654e") });

                        // Assert
                        act.ShouldThrow<Exception>().WithMessage("Error parsing column 0 (Value=cf0ef7ac-b6fe-4e24-aeda-a2b45bb5654e - Object)");
                    }
                }
            }

            public class Enums
                : Query
            {
                [Fact]
                public void Can_use_enums()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        var value = Int32Enum.A;

                        database.Query<Int32Enum>("select @value", new { value }).ShouldAllBeEquivalentTo(new[] { value });
                        database.Query<Int32Enum?>("select @value", new { value = (Int32Enum?)value }).ShouldAllBeEquivalentTo(new[] { (Int32Enum?)value });
                        database.Query<Int32Enum?>("select @value", new { value = (Int32Enum?)null }).ShouldAllBeEquivalentTo(new[] { (Int32Enum?)null });

                        database.Query<GenericEntity<Int32Enum>>("select @value AS Value", new { value }).Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { value });
                        database.Query<GenericEntity<Int32Enum?>>("select @value AS Value", new { value = (Int32Enum?)value }).Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { (Int32Enum?)value });
                        database.Query<GenericEntity<Int32Enum?>>("select @value AS Value", new { value = (Int32Enum?)null }).Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { (Int32Enum?)null });
                    }
                }

                [Fact]
                public void Can_convert_null_to_nullable_enum()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        database.Query<Int32Enum?>("select null").ShouldAllBeEquivalentTo(new[] { (Int32Enum?)null });
                        database.Query<GenericEntity<Int32Enum?>>("select null AS Value").Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { (Int32Enum?)null });
                    }
                }

                [Fact]
                public void Can_convert_int16_to_enum()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        database.Query<Int16Enum>("select cast(42 as smallint)").ShouldAllBeEquivalentTo(new[] { (Int16Enum)42 });
                        database.Query<Int16Enum?>("select cast(42 as smallint)").ShouldAllBeEquivalentTo(new[] { (Int16Enum?)42 });
                        database.Query<Int16Enum?>("select cast(null as smallint)").ShouldAllBeEquivalentTo(new[] { (Int16Enum?)null });

                        database.Query<GenericEntity<Int16Enum>>("select cast(42 as smallint) AS Value").Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { (Int16Enum)42 });
                        database.Query<GenericEntity<Int16Enum?>>("select cast(42 as smallint) AS Value").Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { (Int16Enum?)42 });
                        database.Query<GenericEntity<Int16Enum?>>("select cast(null as smallint) AS Value").Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { (Int16Enum?)null });
                    }
                }

                [Fact]
                public void Can_convert_int32_to_enum()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        database.Query<Int32Enum>("select cast(42 as int)").ShouldAllBeEquivalentTo(new[] { (Int32Enum)42 });
                        database.Query<Int32Enum?>("select cast(42 as int)").ShouldAllBeEquivalentTo(new[] { (Int32Enum?)42 });
                        database.Query<Int32Enum?>("select cast(null as int)").ShouldAllBeEquivalentTo(new[] { (Int32Enum?)null });

                        database.Query<GenericEntity<Int32Enum>>("select cast(42 as int) AS Value").Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { (Int32Enum)42 });
                        database.Query<GenericEntity<Int32Enum?>>("select cast(42 as int) AS Value").Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { (Int32Enum?)42 });
                        database.Query<GenericEntity<Int32Enum?>>("select cast(null as int) AS Value").Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { (Int32Enum?)null });
                    }
                }

                [Fact]
                public void Can_parse_strings_to_integer_backed_enum()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        // database.Query<AnEnum>("select 'B'").ShouldAllBeEquivalentTo(new[] { AnEnum.B });
                        database.Query<GenericEntity<Int32Enum>>("select \'B\' AS Value").Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { Int32Enum.B });
                        database.Query<GenericEntity<Int32Enum>>("select \'b\' AS Value").Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { Int32Enum.B });
                        database.Query<GenericEntity<Int32Enum?>>("select \'B\' AS Value").Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { (Int32Enum?)Int32Enum.B });
                        database.Query<GenericEntity<Int32Enum?>>("select \'b\' AS Value").Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { (Int32Enum?)Int32Enum.B });
                    }
                }
            }

            public class DateTimes
                : Query
            {
                [Fact]
                public void Can_use_datetimes()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        var value = new DateTime(2000, 1, 1);

                        database.Query<DateTime>("select @value", new { value }).ShouldAllBeEquivalentTo(new[] { value });
                        database.Query<DateTime?>("select @value", new { value = (DateTime?)value }).ShouldAllBeEquivalentTo(new[] { (DateTime?)value });
                        database.Query<DateTime?>("select @value", new { value = (DateTime?)null }).ShouldAllBeEquivalentTo(new[] { (DateTime?)null });

                        database.Query<GenericEntity<DateTime>>("select @value AS Value", new { value }).Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { value });
                        database.Query<GenericEntity<DateTime?>>("select @value AS Value", new { value = (DateTime?)value }).Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { (DateTime?)value });
                        database.Query<GenericEntity<DateTime?>>("select @value AS Value", new { value = (DateTime?)null }).Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { (DateTime?)null });
                    }
                }

                /// <summary>
                /// https://connect.microsoft.com/VisualStudio/feedback/details/381934/sqlparameter-dbtype-dbtype-time-sets-the-parameter-to-sqldbtype-datetime-instead-of-sqldbtype-time
                /// </summary>
                [Fact]
                public void Can_use_timespans()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        var value = TimeSpan.FromMinutes(42);

                        database.Query<TimeSpan>("select @value", new { value }).ShouldAllBeEquivalentTo(new[] { value });
                        database.Query<TimeSpan?>("select @value", new { value = (TimeSpan?)value }).ShouldAllBeEquivalentTo(new[] { (TimeSpan?)value });
                        database.Query<TimeSpan?>("select @value", new { value = (TimeSpan?)null }).ShouldAllBeEquivalentTo(new[] { (TimeSpan?)null });

                        database.Query<GenericEntity<TimeSpan>>("select @value AS Value", new { value }).Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { value });
                        database.Query<GenericEntity<TimeSpan?>>("select @value AS Value", new { value = (TimeSpan?)value }).Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { (TimeSpan?)value });
                        database.Query<GenericEntity<TimeSpan?>>("select @value AS Value", new { value = (TimeSpan?)null }).Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { (TimeSpan?)null });
                    }
                }

                /// <summary>
                /// TODO:
                /// </summary>
                [Fact]
                public void TestProcedureWithTimeParameter()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        var p = new DynamicParameters();
                        p.Add("a", TimeSpan.FromHours(10), dbType: DbType.Time);

                        database.Execute(@"CREATE PROCEDURE #TestProcWithTimeParameter
            @a TIME
            AS
            BEGIN
            SELECT @a
            END");

                        Assert.Equal(database.Query<TimeSpan>("#TestProcWithTimeParameter", p, CommandType.StoredProcedure).First(),
                            new TimeSpan(10, 0, 0));
                    }
                }
            }

            public class Binary
                : Query
            {
                [Fact]
                public void Can_use_byte_arrays()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        var value = new byte[] { 1 };

                        database.Query<byte[]>("select @value", new { value }).ShouldAllBeEquivalentTo(new[] { value });
                        database.Query<byte[]>("select @value", new { value = (byte[])null }).ShouldAllBeEquivalentTo(new[] { (byte[])null });

                        database.Query<GenericEntity<byte[]>>("select @value AS Value", new { value }).Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { value });
                        database.Query<GenericEntity<byte[]>>("select @value AS Value", new { value = (byte[])null }).Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { (byte[])null });
                    }
                }
            }

            public class CustomConverters
                : Query
            {
                [Fact]
                public void Errors_when_type_does_not_have_a_converter()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        Action act = () =>
                            database.Query<int>("select count(1) where 1 = @Foo", new { Foo = new UnhandledType(UnhandledTypeOptions.Default) });

                        act.ShouldThrow<NotSupportedException>()
                           .WithMessage("The member Foo of type PeregrineDb.Tests.SharedTypes.UnhandledType cannot be used as a parameter value");
                    }
                }

                [Fact]
                public void Does_not_error_when_unknown_parameter_is_not_used()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        database.Query<int>("select @Bar", new { Foo = new UnhandledType(UnhandledTypeOptions.Default), Bar = 23 }).ShouldAllBeEquivalentTo(new[] { 23 });
                    }
                }

                [Fact(Skip = "Not implemented")]
                public void Can_use_converter_with_value_types()
                {
                    ////using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    ////{
                    ////    TypeProvider.ResetTypeHandlers();
                    ////    TypeProvider.AddTypeHandler(typeof(LocalDate), LocalDateConverter.Default);

                    ////    var value = new LocalDate { Year = 2014, Month = 7, Day = 25 };

                    ////    database.Query<LocalDate>("SELECT @value", new { value }).ShouldAllBeEquivalentTo(value);
                    ////    database.Query<LocalDate?>("SELECT @value", new { value = (LocalDate?)value }).ShouldAllBeEquivalentTo(value);
                    ////    //// TODO: database.Query<LocalDate?>("SELECT @value", new { value = (LocalDate?)null }).ShouldAllBeEquivalentTo((LocalDate?)null);

                    ////    database.Query<GenericEntity<LocalDate>>("SELECT @value AS Value", new { value }).Select(e => e.Value).ShouldAllBeEquivalentTo(value);
                    ////    database.Query<GenericEntity<LocalDate?>>("SELECT @value AS Value", new { value = (LocalDate?)value }).Select(e => e.Value).ShouldAllBeEquivalentTo(value);
                    ////    database.Query<GenericEntity<LocalDate?>>("SELECT @value AS Value", new { value = (LocalDate?)null }).Select(e => e.Value).ShouldAllBeEquivalentTo((LocalDate?)null);

                    ////    TypeProvider.ResetTypeHandlers();
                    ////    TypeProvider.AddTypeHandler(typeof(LocalDate?), LocalDateConverter.Default);

                    ////    database.Query<LocalDate>("SELECT @value", new { value }).ShouldAllBeEquivalentTo(value);
                    ////    database.Query<LocalDate?>("SELECT @value", new { value = (LocalDate?)value }).ShouldAllBeEquivalentTo(value);
                    ////    //// TOODO: database.Query<LocalDate?>("SELECT @value", new { value = (LocalDate?)null }).ShouldAllBeEquivalentTo((LocalDate?)null);

                    ////    database.Query<GenericEntity<LocalDate>>("SELECT @value AS Value", new { value }).Select(e => e.Value).ShouldAllBeEquivalentTo(value);
                    ////    database.Query<GenericEntity<LocalDate?>>("SELECT @value AS Value", new { value = (LocalDate?)value }).Select(e => e.Value).ShouldAllBeEquivalentTo(value);
                    ////    database.Query<GenericEntity<LocalDate?>>("SELECT @value AS Value", new { value = (LocalDate?)null }).Select(e => e.Value).ShouldAllBeEquivalentTo((LocalDate?)null);
                    ////}
                }

                [Fact(Skip = "Not implemented")]
                public void Can_use_converter_with_classes()
                {
                    ////using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    ////{
                    ////    TypeProvider.AddTypeHandler(RatingValueConverter.Default);
                    ////    var foo = database.Query<GenericEntity<RatingValue>>("SELECT 200 AS Value").Single();

                    ////    Assert.Equal(200, foo.Value.Value);
                    ////}
                }

                [Fact(Skip = "Not implemented")]
                public void Can_use_custom_converter_in_interpolated_arguments()
                {
                    ////using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.PostgreSql))
                    ////{
                    ////    // Arrange
                    ////    TypeProvider.AddTypeHandler(new CitextConverter());

                    ////    // Act / Assert
                    ////    var value = (Citext)"Foo";

                    ////    database.Query<Citext>("select @value", new { value }).ShouldAllBeEquivalentTo(new[] { value });
                    ////    database.Query<Citext>("select @value", new { value = (Citext)null }).ShouldAllBeEquivalentTo(new[] { (Citext)null });

                    ////    database.Query<GenericEntity<Citext>>("select @value AS Value", new { value }).Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { value });
                    ////    database.Query<GenericEntity<Citext>>("select @value AS Value", new { value = (Citext)null }).Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { (Citext)null });
                    ////}
                }

                [Fact(Skip = "Not implemented")]
                public void Can_use_converter_to_parse_ienumerable()
                {
                    ////using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    ////{
                    ////    try
                    ////    {
                    ////        TypeProvider.ResetTypeHandlers();
                    ////        TypeProvider.AddTypeHandler(StringListConverter.Default);

                    ////        database.Query<GenericEntity<List<string>>>("SELECT \'Sam,Kyro\' AS Value").Select(e => e.Value)
                    ////                .ShouldAllBeEquivalentTo(new[] { new List<string> { "Sam", "Kyro" } }, o => o.WithStrictOrdering());
                    ////    }
                    ////    finally
                    ////    {
                    ////        TypeProvider.ResetTypeHandlers();
                    ////    }
                    ////}
                }

                [Fact(Skip = "Not implemented")]
                public void Can_use_converter_to_set_parameter_value()
                {
                    ////using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    ////{
                    ////    TypeProvider.ResetTypeHandlers();
                    ////    TypeProvider.AddTypeHandler(StringListConverter.Default);

                    ////    try
                    ////    {
                    ////        database.Query<string>("SELECT @value", new { value = new List<string> { "Sam", "Kyro" } }).ShouldAllBeEquivalentTo(new[] { "Sam,Kyro" });
                    ////    }
                    ////    finally
                    ////    {
                    ////        TypeProvider.ResetTypeHandlers();
                    ////    }
                    ////}
                }
            }

            public class CustomDynamicParameters
                : Query
            {
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

                        var result = database.Query<TestCustomParametersEntity>("select Foo=@foo, Bar=@bar", args).Single();
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
                public void TestAppendingAnonClasses()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        var p = new DynamicParameters();
                        p.AddDynamicParams(new { A = 1, B = 2 });
                        p.AddDynamicParams(new { C = 3, D = 4 });

                        var result = database.Query<TestAppendingAnonClassesEntity>("select @A a,@B b,@C c,@D d", p).Single();

                        Assert.Equal(1, result.A);
                        Assert.Equal(2, result.B);
                        Assert.Equal(3, result.C);
                        Assert.Equal(4, result.D);
                    }
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

                        var result = database.Query<TestAppendingADictionaryEntity>("select @A a, @B b", p).Single();

                        Assert.Equal(1, result.a);
                        Assert.Equal("two", result.b);
                    }
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

                        var result = database.Query<dynamic>("select @A a, @B b", p).Single();

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
                        var list = new[] { 1, 2, 3 };
                        p.AddDynamicParams(new { list });

                        var result = database.Query<int>("select * from (select 1 A union all select 2 union all select 3) X where A = ANY (@list)", p).ToList();

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

                        var result = database.Query<int>("select * from (select 1 A union all select 2 union all select 3) X where A = ANY (@ids)", p)
                                             .ToList();

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
                        var p = new DynamicParameters();
                        var list = new[] { 1, 2, 3 };
                        p.Add("ids", list);

                        var result = database.Query<int>("select * from (select 1 A union all select 2 union all select 3) X where A = ANY (@ids)", p).ToList();

                        Assert.Equal(1, result[0]);
                        Assert.Equal(2, result[1]);
                        Assert.Equal(3, result[2]);
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

                        Assert.Equal("bob", database.Query<string>("set @age = 11 select @name", p).First());
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
                        string result = database.Query<string>("select @name", parameters).First();
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
                        var result = database.Query<int>(@"SELECT id FROM unnest (@myIds) as id", dynamicParams);
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
                        int i = database.Query<int>("select @Foo", args).Single();
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
                        int i = database.Query<int>("select @Foo", args).Single();
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

                        database.Query<dynamic>("SELECT @param1", parameters);
                    }
                }

                [Fact]
                public void Issue182_BindDynamicObjectParametersAndColumns()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        database.Execute("create table #Dyno ([Id] uniqueidentifier primary key, [Name] nvarchar(50) not null, [Foo] bigint not null);");

                        var guid = Guid.NewGuid();
                        var orig = new Dyno { Name = "T Rex", Id = guid, Foo = 123L };
                        var result = database.Execute("insert into #Dyno ([Id], [Name], [Foo]) values (@Id, @Name, @Foo);", orig);

                        var fromDb = database.Query<Dyno>("select * from #Dyno where Id=@value", new { value = guid }).Single();
                        Assert.Equal((Guid)fromDb.Id, guid);
                        Assert.Equal("T Rex", fromDb.Name);
                        Assert.Equal(123L, (long)fromDb.Foo);
                    }
                }

                [Fact]
                public void Issue220_InParameterCanBeSpecifiedInAnyCase()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.PostgreSql))
                    {
                        // note this might fail if your database server is case-sensitive
                        Assert.Equal(new[] { 1 },
                            database.Query<int>("select * from (select 1 as Id) as X where Id = ANY (@ids)", new { Ids = new[] { 1 } }));
                    }
                }
            }
        }
    }
}