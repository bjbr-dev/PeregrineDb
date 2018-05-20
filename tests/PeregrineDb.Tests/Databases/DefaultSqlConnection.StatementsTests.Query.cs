namespace PeregrineDb.Tests.Databases
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Data.SqlClient;
    using System.Diagnostics;
    using System.Dynamic;
    using System.Globalization;
    using System.Linq;
    using FluentAssertions;
    using PeregrineDb.Mapping;
    using PeregrineDb.Tests.SharedTypes;
    using PeregrineDb.Tests.Testing;
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
                public void Uses_public_parameterless_constructor()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        // Act
                        var result = database.Query<PublicParameterlessConstructor>($"select 1 A").First();

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
                        var result = database.Query<MultipleConstructors>($"select 1 A").First();

                        // Assert
                        result.A.Should().Be(1);
                    }
                }

                [Fact]
                public void Errors_when_no_public_constructor_is_found()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        // Act
                        Action act = () => database.Query<NoPublicConstructor>($"select 1 as A");

                        // Assert
                        act.ShouldThrow<InvalidOperationException>()
                           .WithMessage("PeregrineDb.Tests.SharedTypes.NoPublicConstructor must have a public parameterless constructor");
                    }
                }

                [Fact]
                public void Errors_when_no_parameterless_constructor_is_found()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        // Act
                        Action act = () => database.Query<NoParameterlessConstructor>($"select 1 as A");

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
                        var result = database.Query<InheritanceTest2>($"select 'One' as Derived1, 'Two' as Base1").First();

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
                        var order = database.Query<AbstractInheritance.ConcreteOrder>($"select 3 [Public], 4 Concrete").First();

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
                        database.Query<int?>($"print 'not a query'").Should().BeEmpty();
                        database.Query<GenericEntity<int?>>($"print 'not a query'").Should().BeEmpty();
                    }
                }

                [Fact]
                public void Returns_empty_set_when_query_returns_no_rows()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        // Act / Assert
                        database.Query<int?>($"SELECT 5 WHERE 1 = 0").Should().BeEmpty();
                    }
                }

                [FactLongRunning]
                public void Allows_for_a_long_running_query()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        // Act
                        var watch = Stopwatch.StartNew();
                        var i = database.Query<int>($"waitfor delay '00:01:00'; select 42;", 300).Single();
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
                        Action act = () => database.Query<int>($"waitfor delay '00:01:00'; select 42;", 1);

                        // Assert
                        act.ShouldThrow<SqlException>().And.Message.Should().Contain("Timeout expired");
                    }
                }

                [Fact]
                public void Does_not_set_internal_properties()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        // Act / Assert
                        Assert.Equal(default, database.Query<TestObj>($"select 10 as [Internal]").First()._internal);
                    }
                }

                [Fact]
                public void Does_not_set_private_properties()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        // Act / Assert
                        Assert.Equal(default, database.Query<TestObj>($"select 10 as [Priv]").First()._priv);
                    }
                }

                [Fact]
                public void Does_not_populate_fields()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        // Act
                        var data = database.Query<TestFieldsEntity>($"select a=1,b=2,c=3").Single();

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
                        database.Execute($"create table #dog(Age int, Name nvarchar(max)) insert #dog values(1, 'Alf')");

                        FormattableString sql = $"select * from #dog";

                        var beforeResult = database.Query<Dog>(sql).Single();
                        Assert.Equal("Alf", beforeResult.Name);
                        Assert.Equal(1, beforeResult.Age);
                        
                        database.Execute($"alter table #dog drop column Name");

                        // Act
                        var afterResult = database.Query<Dog>(sql).Single();

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
                        var guid = Guid.NewGuid();

                        // Act
                        var dog = database.Query<Dog>($"select '' as Extra, 1 as Age, 'Rover' as Name, Id = {guid}");

                        // Assert
                        dog.ShouldAllBeEquivalentTo(new[]
                            {
                                new Dog { Id = guid, Age = 1, Name = "Rover", Weight = null }
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

                            database.RawQuery<int>("select @pid", new { PId = 1 }).Single();
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
                        Action act =()=> database.RawQuery<dynamic>("Select @A, @B", parameters);

                        // Assert
                        act.ShouldThrow<InvalidOperationException>()
                           .WithMessage("An enumerable sequence of parameters (arrays, lists, etc) is not allowed in this context");
                    }
                }

                [Fact]
                public void Ignores_indexer_properties()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        // Act
                        var result = database.RawQuery<ParameterWithIndexer>("SELECT @A as A", new ParameterWithIndexer { A = 5 }).Single();

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
                        database.Query<string>($"select 'a' a union select 'b'")
                                .ShouldAllBeEquivalentTo(new[] { "a", "b" }, o => o.WithStrictOrdering());

                        database.Query<GenericEntity<string>>($"select 'a' Value union select 'b'")
                                .Select(e => e.Value)
                                .ShouldAllBeEquivalentTo(new[] { "a", "b" }, o => o.WithStrictOrdering());
                    }
                }

                [Fact]
                public void Can_query_null_string()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        database.Query<string>($"select CAST (null as nvarchar(MAX))")
                                .ShouldAllBeEquivalentTo(new[] { (string)null }, o => o.WithStrictOrdering());

                        database.Query<GenericEntity<string>>($"select CAST (null as nvarchar(MAX))")
                                .Select(e => e.Value)
                                .ShouldAllBeEquivalentTo(new[] { (string)null }, o => o.WithStrictOrdering());
                    }
                }

                [Fact]
                public void Can_query_naked_empty_string()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        database.Query<string>($"select ''")
                                .ShouldAllBeEquivalentTo(new[] { string.Empty }, o => o.WithStrictOrdering());

                        database.Query<GenericEntity<string>>($"select '' Value")
                                .Select(e => e.Value)
                                .ShouldAllBeEquivalentTo(new[] { string.Empty }, o => o.WithStrictOrdering());
                    }
                }

                [Fact]
                public void Parameter_can_have_a_string()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.PostgreSql))
                    {
                        database.Query<string>($"select {"a"}").ShouldAllBeEquivalentTo(new[] { "a" });
                        database.RawQuery<string>("select @Value", GenericEntity.From("a")).ShouldAllBeEquivalentTo(new[] { "a" });
                    }
                }

                [Fact]
                public void Parameter_can_have_a_null_string()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.PostgreSql))
                    {
                        database.Query<string>($"select {null}::text").ShouldAllBeEquivalentTo(new[] { (string)null });
                        database.RawQuery<string>("select @Value", GenericEntity.From<string>(null)).ShouldAllBeEquivalentTo(new[] { (string)null });
                    }
                }

                [Fact]
                public void Parameter_can_have_a_list_of_strings()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.PostgreSql))
                    {
                        var values = new[] { "a", "b", "c" }.ToList();
                        database.Query<string>($"select * from (select 'a' as x union all select 'b' union all select 'c') as T where x = ANY ({values})")
                                .ShouldAllBeEquivalentTo(values, o => o.WithStrictOrdering());

                        var emptyList = new string[0].ToList();
                        database.Query<string>($"select * from (select 'a' as x union all select 'b' union all select 'c') as T where x = ANY ({emptyList})")
                                .Should().BeEmpty();
                    }
                }

                [Fact]
                public void Parameter_can_have_massive_string()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        var str = new string('X', 20000);
                        database.Query<string>($"select {str}").ShouldAllBeEquivalentTo(new[] { str });
                    }
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
                        database.Query<char>($"select {value}").ShouldAllBeEquivalentTo(new[] { value });
                        database.Query<char?>($"select {value}").ShouldAllBeEquivalentTo(new[] { value });
                        database.Query<char?>($"select {null}").ShouldAllBeEquivalentTo(new[] { (char?)null });

                        database.Query<GenericEntity<char>>($"select {value} as Value").Select(c => c.Value).ShouldAllBeEquivalentTo(new[] { value });
                        database.Query<GenericEntity<char?>>($"select {value} as Value").Select(c => c.Value).ShouldAllBeEquivalentTo(new[] { value });
                        database.Query<GenericEntity<char?>>($"select {null} as Value").Select(c => c.Value).ShouldAllBeEquivalentTo(new[] { (char?)null });
                    }
                }

                [Fact]
                public void Can_read_single_character_strings()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        database.Query<char>($"select 'f'").ShouldAllBeEquivalentTo(new[] { 'f' });
                        database.Query<GenericEntity<char>>($"select 'f' as Value").Select(c => c.Value).ShouldAllBeEquivalentTo(new[] { 'f' });

                        database.Query<char?>($"select 'f'").ShouldAllBeEquivalentTo(new[] { 'f' });
                        database.Query<GenericEntity<char?>>($"select 'f' as Value").Select(c => c.Value).ShouldAllBeEquivalentTo(new[] { 'f' });
                    }
                }

                [Fact]
                public void Throws_exception_if_string_has_more_than_one_character()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        Action act = () => database.Query<char>($"select 'foo'");
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
                        database.Query<short>($"select cast({(short)42} as smallint)").ShouldAllBeEquivalentTo(new[] { (short)42 });
                        database.Query<short?>($"select cast({(short?)42} as smallint)").ShouldAllBeEquivalentTo(new[] { (short?)42 });
                        database.Query<short?>($"select cast({(short?)null} as smallint)").ShouldAllBeEquivalentTo(new[] { (short?)null });

                        database.Query<GenericEntity<short>>($"select cast({(short)42} as smallint) AS Value").Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { (short)42 });
                        database.Query<GenericEntity<short?>>($"select cast({(short?)42} as smallint) AS Value").Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { (short?)42 });
                        database.Query<GenericEntity<short?>>($"select cast({(short?)null} as smallint) AS Value").Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { (short?)null });
                    }
                }

                [Fact]
                public void Can_use_int32()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        database.Query<int>($"select cast({42} as int)").ShouldAllBeEquivalentTo(new[] { 42 });
                        database.Query<int?>($"select cast({(int?)42} as int)").ShouldAllBeEquivalentTo(new[] { (int?)42 });
                        database.Query<int?>($"select cast({(int?)null} as int)").ShouldAllBeEquivalentTo(new[] { (int?)null });

                        database.Query<GenericEntity<int>>($"select cast({42} as int) AS Value").Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { 42 });
                        database.Query<GenericEntity<int?>>($"select cast({(int?)42} as int) AS Value").Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { (int?)42 });
                        database.Query<GenericEntity<int?>>($"select cast({(int?)null} as int) AS Value").Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { (int?)null });
                    }
                }

                [Fact]
                public void Can_read_list_of_int32()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        database.Query<int>($"select 1 union all select 2 union all select 3")
                                .ShouldAllBeEquivalentTo(new[] { 1, 2, 3 }, o => o.WithStrictOrdering());
                    }
                }

                [Fact]
                public void Can_use_int64()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        database.Query<long>($"select cast({42L} as bigint)").ShouldAllBeEquivalentTo(new[] { 42L });
                        database.Query<long?>($"select cast({(long?)42L} as bigint)").ShouldAllBeEquivalentTo(new[] { (long?)42L });
                        database.Query<long?>($"select cast({(long?)null} as bigint)").ShouldAllBeEquivalentTo(new[] { (long?)null });

                        database.Query<GenericEntity<long>>($"select cast({42L} as bigint) AS Value").Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { 42L });
                        database.Query<GenericEntity<long?>>($"select cast({(long?)42L} as bigint) AS Value").Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { (long?)42L });
                        database.Query<GenericEntity<long?>>($"select cast({(long?)null} as bigint) AS Value").Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { (long?)null });
                    }
                }

                [Fact]
                public void Converts_int64_to_int32()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        database.Query<int>($"select cast(42 as bigint) as Value").ShouldAllBeEquivalentTo(new[] { 42 });
                        database.Query<GenericEntity<int>>($"select cast(42 as bigint) as Value").Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { 42 });
                    }
                }

                [Fact]
                public void Throws_error_if_int64_downward_conversion_is_out_of_range()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        Action act = () => database.Query<int>($"select {(long)int.MaxValue + 2}");
                        act.ShouldThrow<OverflowException>().WithMessage("Value was either too large or too small for an Int32.");

                        act = () => database.Query<GenericEntity<int>>($"select cast({(long)int.MaxValue + 2} as bigint) as Value");
                        act.ShouldThrow<InvalidOperationException>().WithMessage("Error parsing column 0 (Value=2147483649 - Int64)");
                    }
                }

                [Fact]
                public void Parameter_can_have_list_of_int32()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.PostgreSql))
                    {
                        var values = new[] { 1, 2, 3 }.AsEnumerable();
                        database.Query<int>($"select * from (select 1 as Id union all select 2 union all select 3) as X where Id = ANY ({values})")
                                .ShouldAllBeEquivalentTo(new[] { 1, 2, 3 }, o => o.WithStrictOrdering());
                    }
                }

                [Fact]
                public void Parameter_can_have_empty_list_of_int32()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.PostgreSql))
                    {
                        var values = new int[0];
                        database.Query<int>($"select * from (select 1 as Id union all select 2 union all select 3) as X where Id = ANY ({values})")
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
                        database.Query<double>($"select {0.1d}").ShouldAllBeEquivalentTo(new[] { 0.1d });
                        database.Query<double?>($"select {(double?)0.1d}").ShouldAllBeEquivalentTo(new[] { (double?)0.1d });
                        database.Query<double?>($"select {(double?)null}").ShouldAllBeEquivalentTo(new[] { (double?)null });

                        database.Query<GenericEntity<double>>($"select {0.1d} AS Value").Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { 0.1d });
                        database.Query<GenericEntity<double?>>($"select {(double?)0.1d} AS Value").Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { (double?)0.1d });
                        database.Query<GenericEntity<double?>>($"select {(double?)null} AS Value").Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { (double?)null });
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
                        database.Query<decimal>($"select {11.884M}").ShouldAllBeEquivalentTo(new[] { 11.884M });
                        database.Query<decimal?>($"select {(decimal?)11.884M}").ShouldAllBeEquivalentTo(new[] { (decimal?)11.884M });
                        database.Query<decimal?>($"select {(decimal?)null}").ShouldAllBeEquivalentTo(new[] { (decimal?)null });

                        database.Query<GenericEntity<decimal>>($"select {11.884M} AS Value").Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { 11.884M });
                        database.Query<GenericEntity<decimal?>>($"select {(decimal?)11.884M} AS Value").Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { (decimal?)11.884M });
                        database.Query<GenericEntity<decimal?>>($"select {(decimal?)null} AS Value").Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { (decimal?)null });
                    }
                }

                [Fact]
                public void Converts_doubles_to_decimals()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        database.Query<decimal>($"select cast(1 as float)").ShouldAllBeEquivalentTo(new[] { 1.0M });
                        database.Query<GenericEntity<decimal>>($"select cast(1 as float) as Value").Select(e => e.Value)
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
                        database.Query<decimal?>($"select cast(null as float)").ShouldAllBeEquivalentTo(new[] { (decimal?)null });
                        database.Query<GenericEntity<decimal>>($"select cast(null as float) as Value").Select(e => e.Value)
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
                        database.Query<bool>($"select {true}").ShouldAllBeEquivalentTo(new[] { true });
                        database.Query<bool?>($"select {(bool?)true}").ShouldAllBeEquivalentTo(new[] { (bool?)true });
                        database.Query<bool?>($"select {(bool?)null}").ShouldAllBeEquivalentTo(new[] { (bool?)null });

                        database.Query<GenericEntity<bool>>($"select {true} AS Value").Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { true });
                        database.Query<GenericEntity<bool?>>($"select {(bool?)true} AS Value").Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { (bool?)true });
                        database.Query<GenericEntity<bool?>>($"select {(bool?)null} AS Value").Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { (bool?)null });
                    }
                }

                [Fact]
                public void Converts_sql_bit_to_boolean()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        database.Query<bool>($"select CAST(1 as BIT)").ShouldAllBeEquivalentTo(new[] { true });
                        database.Query<bool>($"select CAST(0 as BIT)").ShouldAllBeEquivalentTo(new[] { false });

                        database.Query<bool?>($"select CAST(1 as BIT)").ShouldAllBeEquivalentTo(new[] { true });
                        database.Query<bool?>($"select CAST(0 as BIT)").ShouldAllBeEquivalentTo(new[] { false });
                        database.Query<bool?>($"select CAST(NULL as BIT)").ShouldAllBeEquivalentTo(new[] { (bool?)null });

                        database.Query<GenericEntity<bool>>($"select CAST(1 as BIT) AS Value").Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { true });
                        database.Query<GenericEntity<bool>>($"select CAST(0 as BIT) AS Value").Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { false });

                        database.Query<GenericEntity<bool?>>($"select CAST(1 as BIT) AS Value").Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { true });
                        database.Query<GenericEntity<bool?>>($"select CAST(0 as BIT) AS Value").Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { false });
                        database.Query<GenericEntity<bool?>>($"select CAST(NULL as BIT) AS Value").Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { (bool?)null });
                    }
                }

                [Fact]
                public void Converts_sql_tinyint_to_boolean()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        database.Query<bool>($"select CAST(1 as tinyint)").ShouldAllBeEquivalentTo(new[] { true });
                        database.Query<bool>($"select CAST(0 as tinyint)").ShouldAllBeEquivalentTo(new[] { false });

                        database.Query<bool?>($"select CAST(1 as tinyint)").ShouldAllBeEquivalentTo(new[] { true });
                        database.Query<bool?>($"select CAST(0 as tinyint)").ShouldAllBeEquivalentTo(new[] { false });
                        database.Query<bool?>($"select CAST(NULL as tinyint)").ShouldAllBeEquivalentTo(new[] { (bool?)null });

                        database.Query<GenericEntity<bool>>($"select CAST(1 as tinyint) AS Value").Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { true });
                        database.Query<GenericEntity<bool>>($"select CAST(0 as tinyint) AS Value").Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { false });

                        database.Query<GenericEntity<bool?>>($"select CAST(1 as tinyint) AS Value").Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { true });
                        database.Query<GenericEntity<bool?>>($"select CAST(0 as tinyint) AS Value").Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { false });
                        database.Query<GenericEntity<bool?>>($"select CAST(NULL as tinyint) AS Value").Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { (bool?)null });
                    }
                }

                [Fact]
                public void Converts_pg_booleans_to_boolean()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.PostgreSql))
                    {
                        database.Query<bool>($"select TRUE").ShouldAllBeEquivalentTo(new[] { true });
                        database.Query<bool>($"select FALSE").ShouldAllBeEquivalentTo(new[] { false });

                        database.Query<bool?>($"select TRUE").ShouldAllBeEquivalentTo(new[] { true });
                        database.Query<bool?>($"select FALSE").ShouldAllBeEquivalentTo(new[] { false });
                        database.Query<bool?>($"select NULL::boolean").ShouldAllBeEquivalentTo(new[] { (bool?)null });

                        database.Query<GenericEntity<bool>>($"select TRUE AS Value").Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { true });
                        database.Query<GenericEntity<bool>>($"select FALSE AS Value").Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { false });

                        database.Query<GenericEntity<bool?>>($"select TRUE AS Value").Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { true });
                        database.Query<GenericEntity<bool?>>($"select FALSE AS Value").Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { false });
                        database.Query<GenericEntity<bool?>>($"select NULL::boolean AS Value").Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { (bool?)null });
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

                        database.Query<Guid>($"select {value}").ShouldAllBeEquivalentTo(new[] { value });
                        database.Query<Guid?>($"select {(Guid?)value}").ShouldAllBeEquivalentTo(new[] { (Guid?)value });
                        database.Query<Guid?>($"select {(Guid?)null}").ShouldAllBeEquivalentTo(new[] { (Guid?)null });

                        database.Query<GenericEntity<Guid>>($"select {value} AS Value").Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { value });
                        database.Query<GenericEntity<Guid?>>($"select {(Guid?)value} AS Value").Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { (Guid?)value });
                        database.Query<GenericEntity<Guid?>>($"select {(Guid?)null} AS Value").Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { (Guid?)null });
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

                        database.Query<Int32Enum>($"select {value}").ShouldAllBeEquivalentTo(new[] { value });
                        database.Query<Int32Enum?>($"select {(Int32Enum?)value}").ShouldAllBeEquivalentTo(new[] { (Int32Enum?)value });
                        database.Query<Int32Enum?>($"select {(Int32Enum?)null}").ShouldAllBeEquivalentTo(new[] { (Int32Enum?)null });

                        database.Query<GenericEntity<Int32Enum>>($"select {value} AS Value").Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { value });
                        database.Query<GenericEntity<Int32Enum?>>($"select {(Int32Enum?)value} AS Value").Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { (Int32Enum?)value });
                        database.Query<GenericEntity<Int32Enum?>>($"select {(Int32Enum?)null} AS Value").Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { (Int32Enum?)null });
                    }
                }

                [Fact]
                public void Can_convert_null_to_nullable_enum()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        database.Query<Int32Enum?>($"select null").ShouldAllBeEquivalentTo(new[] { (Int32Enum?)null });
                        database.Query<GenericEntity<Int32Enum?>>($"select null AS Value").Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { (Int32Enum?)null });
                    }
                }

                [Fact]
                public void Can_convert_int16_to_enum()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        database.Query<Int16Enum>($"select cast(42 as smallint)").ShouldAllBeEquivalentTo(new[] { (Int16Enum)42 });
                        database.Query<Int16Enum?>($"select cast(42 as smallint)").ShouldAllBeEquivalentTo(new[] { (Int16Enum?)42 });
                        database.Query<Int16Enum?>($"select cast(null as smallint)").ShouldAllBeEquivalentTo(new[] { (Int16Enum?)null });

                        database.Query<GenericEntity<Int16Enum>>($"select cast(42 as smallint) AS Value").Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { (Int16Enum)42 });
                        database.Query<GenericEntity<Int16Enum?>>($"select cast(42 as smallint) AS Value").Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { (Int16Enum?)42 });
                        database.Query<GenericEntity<Int16Enum?>>($"select cast(null as smallint) AS Value").Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { (Int16Enum?)null });
                    }
                }

                [Fact]
                public void Can_convert_int32_to_enum()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        database.Query<Int32Enum>($"select cast(42 as int)").ShouldAllBeEquivalentTo(new[] { (Int32Enum)42 });
                        database.Query<Int32Enum?>($"select cast(42 as int)").ShouldAllBeEquivalentTo(new[] { (Int32Enum?)42 });
                        database.Query<Int32Enum?>($"select cast(null as int)").ShouldAllBeEquivalentTo(new[] { (Int32Enum?)null });

                        database.Query<GenericEntity<Int32Enum>>($"select cast(42 as int) AS Value").Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { (Int32Enum)42 });
                        database.Query<GenericEntity<Int32Enum?>>($"select cast(42 as int) AS Value").Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { (Int32Enum?)42 });
                        database.Query<GenericEntity<Int32Enum?>>($"select cast(null as int) AS Value").Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { (Int32Enum?)null });
                    }
                }

                [Fact]
                public void Can_parse_strings_to_integer_backed_enum()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        // database.Query<AnEnum>($"select 'B'").ShouldAllBeEquivalentTo(new[] { AnEnum.B });
                        database.Query<GenericEntity<Int32Enum>>($"select 'B' AS Value").Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { Int32Enum.B });
                        database.Query<GenericEntity<Int32Enum>>($"select 'b' AS Value").Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { Int32Enum.B });
                        database.Query<GenericEntity<Int32Enum?>>($"select 'B' AS Value").Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { (Int32Enum?)Int32Enum.B });
                        database.Query<GenericEntity<Int32Enum?>>($"select 'b' AS Value").Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { (Int32Enum?)Int32Enum.B });
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

                        database.Query<DateTime>($"select {value}").ShouldAllBeEquivalentTo(new[] { value });
                        database.Query<DateTime?>($"select {(DateTime?)value}").ShouldAllBeEquivalentTo(new[] { (DateTime?)value });
                        database.Query<DateTime?>($"select {(DateTime?)value}").ShouldAllBeEquivalentTo(new[] { (DateTime?)value });

                        database.Query<GenericEntity<DateTime>>($"select {value} AS Value").Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { value });
                        database.Query<GenericEntity<DateTime?>>($"select {(DateTime?)value} AS Value").Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { (DateTime?)value });
                        database.Query<GenericEntity<DateTime?>>($"select {(DateTime?)value} AS Value").Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { (DateTime?)value });
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

                        database.Query<TimeSpan>($"select {value}").ShouldAllBeEquivalentTo(new[] { value });
                        database.Query<TimeSpan?>($"select {(TimeSpan?)value}").ShouldAllBeEquivalentTo(new[] { (TimeSpan?)value });
                        database.Query<TimeSpan?>($"select {(TimeSpan?)null}").ShouldAllBeEquivalentTo(new[] { (TimeSpan?)null });

                        database.Query<GenericEntity<TimeSpan>>($"select {value} AS Value").Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { value });
                        database.Query<GenericEntity<TimeSpan?>>($"select {(TimeSpan?)value} AS Value").Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { (TimeSpan?)value });
                        database.Query<GenericEntity<TimeSpan?>>($"select {(TimeSpan?)null} AS Value").Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { (TimeSpan?)null });
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

                        database.Query<byte[]>($"select {value}").ShouldAllBeEquivalentTo(new[] { value });
                        database.Query<byte[]>($"select {null}").ShouldAllBeEquivalentTo(new[] { (byte[])null });

                        database.Query<GenericEntity<byte[]>>($"select {value} AS Value").Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { value });
                        database.Query<GenericEntity<byte[]>>($"select {null} AS Value").Select(e => e.Value).ShouldAllBeEquivalentTo(new[] { (byte[])null });
                    }
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

                        var result = database.RawQuery<int>("select * from (select 1 A union all select 2 union all select 3) X where A = ANY (@list)", p)
                                             .ToList();

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

                        var result = database.RawQuery<int>("select * from (select 1 A union all select 2 union all select 3) X where A = ANY (@ids)", p)
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
                        DynamicParameters p = new DynamicParameters();
                        var list = new int[] { 1, 2, 3 };
                        p.Add("ids", list);

                        var result = database.RawQuery<int>("select * from (select 1 A union all select 2 union all select 3) X where A = ANY (@ids)", p)
                                             .ToList();

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
            }

            public class UnhandledMethods
                : Query
            {
                [Fact]
                public void TestUnexpectedDataMessage()
                {
                    using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                    {
                        Action act = () =>
                            database.RawQuery<int>("select count(1) where 1 = @Foo", new { Foo = new UnhandledType(UnhandledTypeOptions.Default) });

                        act.ShouldThrow<Exception>()
                           .WithMessage("The member Foo of type PeregrineDb.Tests.SharedTypes.UnhandledType cannot be used as a parameter value");
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
            }
        }
    }
}