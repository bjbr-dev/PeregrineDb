namespace PeregrineDb.Tests.Databases.Mapper
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Data;
    using System.Data.SqlClient;
    using System.Dynamic;
    using System.Globalization;
    using System.Linq;
    using System.Text.RegularExpressions;
    using PeregrineDb.Databases.Mapper;
    using PeregrineDb.Tests.Databases.Mapper.SharedTypes;
    using PeregrineDb.Tests.Utils;
    using Xunit;
    using PSqlCommand = PeregrineDb.SqlCommand;

    public class ParameterTests
    {
        public class DbParams
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

        private static List<Microsoft.SqlServer.Server.SqlDataRecord> CreateSqlDataRecordList(IEnumerable<int> numbers)
        {
            var number_list = new List<Microsoft.SqlServer.Server.SqlDataRecord>();

            // Create an SqlMetaData object that describes our table type.
            Microsoft.SqlServer.Server.SqlMetaData[] tvp_definition = { new Microsoft.SqlServer.Server.SqlMetaData("n", SqlDbType.Int) };

            foreach (int n in numbers)
            {
                // Create a new record, using the metadata array above.
                var rec = new Microsoft.SqlServer.Server.SqlDataRecord(tvp_definition);
                rec.SetInt32(0, n); // Set the value.
                number_list.Add(rec); // Add it to the list.
            }

            return number_list;
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
                var sqlCommand = (SqlCommand)command;
                sqlCommand.CommandType = CommandType.StoredProcedure;

                var number_list = CreateSqlDataRecordList(this.numbers);

                // Add the table parameter.
                var p = sqlCommand.Parameters.Add("ints", SqlDbType.Structured);
                p.Direction = ParameterDirection.Input;
                p.TypeName = "int_list_type";
                p.Value = number_list;
            }
        }

        private class IntCustomParam : ICustomQueryParameter
        {
            private readonly IEnumerable<int> numbers;

            public IntCustomParam(IEnumerable<int> numbers)
            {
                this.numbers = numbers;
            }

            public void AddParameter(IDbCommand command, string name)
            {
                var sqlCommand = (SqlCommand)command;
                sqlCommand.CommandType = CommandType.StoredProcedure;

                var number_list = CreateSqlDataRecordList(this.numbers);

                // Add the table parameter.
                var p = sqlCommand.Parameters.Add(name, SqlDbType.Structured);
                p.Direction = ParameterDirection.Input;
                p.TypeName = "int_list_type";
                p.Value = number_list;
            }
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

        // http://code.google.com/p/dapper-dot-net/issues/detail?id=70
        // https://connect.microsoft.com/VisualStudio/feedback/details/381934/sqlparameter-dbtype-dbtype-time-sets-the-parameter-to-sqldbtype-datetime-instead-of-sqldbtype-time

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
            using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
            {
                Assert.Equal(
                    new[] { 1, 2, 3 },
                    database.Query<int>(
                        $"select * from (select 1 as Id union all select 2 union all select 3) as X where Id in {new int[] { 1, 2, 3 }.AsEnumerable()}")
                );
            }
        }

        [Fact]
        public void PassInEmptyIntArray()
        {
            using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
            {
                Assert.Equal(
                    new int[0],
                    database.Query<int>($"select * from (select 1 as Id union all select 2 union all select 3) as X where Id in {new int[0]}")
                );
            }
        }

        [Fact]
        public void TestExecuteCommandWithHybridParameters()
        {
            using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
            {
                var p = new DynamicParameters(new { a = 1, b = 2 });
                p.Add("c", dbType: DbType.Int32, direction: ParameterDirection.Output);

                var sqlCommand = new PSqlCommand("set @c = @a + @b", p);
                database.Execute(in sqlCommand);
                Assert.Equal(3, p.Get<int>("@c"));
            }
        }

        [Fact]
        public void GuidIn_SO_24177902()
        {
            using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
            {
                // invent and populate
                Guid a = Guid.NewGuid(), b = Guid.NewGuid(), c = Guid.NewGuid(), d = Guid.NewGuid();
                database.Execute($"create table #foo (i int, g uniqueidentifier)");

                var insertCommand = new PSqlCommand("insert #foo(i,g) values(@i,@g)", new[]
                    {
                        new { i = 1, g = a }, new { i = 2, g = b },
                        new { i = 3, g = c }, new { i = 4, g = d }
                    });
                database.Execute(in insertCommand);

                // check that rows 2&3 yield guids b&c
                var guids = database.Query<Guid>($"select g from #foo where i in (2,3)").ToArray();
                guids.Length.Equals(2);
                guids.Contains(a).Equals(false);
                guids.Contains(b).Equals(true);
                guids.Contains(c).Equals(true);
                guids.Contains(d).Equals(false);

                // in query on the guids
                var rows = database.Query<dynamic>($"select * from #foo where g in {guids} order by i")
                               .Select(row => new { i = (int)row.i, g = (Guid)row.g }).ToArray();
                rows.Length.Equals(2);
                rows[0].i.Equals(2);
                rows[0].g.Equals(b);
                rows[1].i.Equals(3);
                rows[1].g.Equals(c);
            }
        }

        [Fact]
        public void TestParameterInclusionNotSensitiveToCurrentCulture()
        {
            using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
            {
                // note this might fail if your database server is case-sensitive
                CultureInfo current = ActiveCulture;
                try
                {
                    ActiveCulture = new CultureInfo("tr-TR");

                    var sqlCommand = new PSqlCommand("select @pid", new { PId = 1 });
                    database.Query<int>(in sqlCommand).Single();
                }
                finally
                {
                    ActiveCulture = current;
                }
            }
        }

        protected static CultureInfo ActiveCulture
        {
            get => CultureInfo.CurrentCulture;
            set => CultureInfo.CurrentCulture = value;
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

        [Fact(Skip = "Interface not implemented")]
        public void TestTVPWithAnonymousObject()
        {
            using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
            {
                ////try
                ////{
                ////    database.Execute($"CREATE TYPE int_list_type AS TABLE (n int NOT NULL PRIMARY KEY)");
                ////    database.Execute($"CREATE PROC get_ints @integers int_list_type READONLY AS select * from @integers");

                ////    var nums = database.Query<int>("get_ints", new { integers = new IntCustomParam(new int[] { 1, 2, 3 }) },
                ////        commandType: CommandType.StoredProcedure).ToList();
                ////    Assert.Equal(1, nums[0]);
                ////    Assert.Equal(2, nums[1]);
                ////    Assert.Equal(3, nums[2]);
                ////    Assert.Equal(3, nums.Count);
                ////}
                ////finally
                ////{
                ////    try
                ////    {
                ////        database.Execute("DROP PROC get_ints");
                ////    }
                ////    finally
                ////    {
                ////        database.Execute("DROP TYPE int_list_type");
                ////    }
                ////}
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

                    var command = new PSqlCommand("get_ints", new IntDynamicParam(new int[] { 1, 2, 3 }));
                    var nums = database.Query<int>(in command).ToList();
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
                var command = new PSqlCommand("select Foo=@foo, Bar=@bar", args);
                var result = database.Query<dynamic>(in command).Single();
                int foo = result.Foo;
                string bar = result.Bar;
                Assert.Equal(123, foo);
                Assert.Equal("abc", bar);
            }
        }

        [Fact]
        public void TestDynamicParamNullSupport()
        {
            using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
            {
                var p = new DynamicParameters();

                p.Add("@b", dbType: DbType.Int32, direction: ParameterDirection.Output);
                var command = new PSqlCommand("select @b = null", p);
                database.Execute(in command);

                Assert.Null(p.Get<int?>("@b"));
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

                var command = new PSqlCommand("select @A a,@B b,@C c,@D d", p);
                var result = database.Query<dynamic>(in command).Single();

                Assert.Equal(1, (int)result.a);
                Assert.Equal(2, (int)result.b);
                Assert.Equal(3, (int)result.c);
                Assert.Equal(4, (int)result.d);
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

                var command = new PSqlCommand("select @A a, @B b", p);
                var result = database.Query<dynamic>(in command).Single();

                Assert.Equal(1, (int)result.a);
                Assert.Equal("two", (string)result.b);
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

                var command = new PSqlCommand("select @A a, @B b", p);
                var result = database.Query<dynamic>(in command).Single();

                Assert.Equal(1, (int)result.a);
                Assert.Equal("two", (string)result.b);
            }
        }

        [Fact]
        public void TestAppendingAList()
        {
            using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
            {
                var p = new DynamicParameters();
                var list = new int[] { 1, 2, 3 };
                p.AddDynamicParams(new { list });

                var command = new PSqlCommand("select * from (select 1 A union all select 2 union all select 3) X where A in @list", p);
                var result = database.Query<int>(in command).ToList();

                Assert.Equal(1, result[0]);
                Assert.Equal(2, result[1]);
                Assert.Equal(3, result[2]);
            }
        }

        [Fact]
        public void TestAppendingAListAsDictionary()
        {
            using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
            {
                var p = new DynamicParameters();
                var list = new int[] { 1, 2, 3 };
                var args = new Dictionary<string, object> { ["ids"] = list };
                p.AddDynamicParams(args);

                var command = new PSqlCommand("select * from (select 1 A union all select 2 union all select 3) X where A in @ids", p);
                var result = database.Query<int>(in command).ToList();

                Assert.Equal(1, result[0]);
                Assert.Equal(2, result[1]);
                Assert.Equal(3, result[2]);
            }
        }

        [Fact]
        public void TestAppendingAListByName()
        {
            using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
            {
                DynamicParameters p = new DynamicParameters();
                var list = new int[] { 1, 2, 3 };
                p.Add("ids", list);

                var command = new PSqlCommand("select * from (select 1 A union all select 2 union all select 3) X where A in @ids", p);
                var result = database.Query<int>(in command).ToList();

                Assert.Equal(1, result[0]);
                Assert.Equal(2, result[1]);
                Assert.Equal(3, result[2]);
            }
        }

        [Fact]
        public void ParameterizedInWithOptimizeHint()
        {
            using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
            {
                const string sql = @"
select count(1)
from(
    select 1 as x
    union all select 2
    union all select 5) y
where y.x in @vals
option (optimize for (@vals unKnoWn))";
                var command = new PSqlCommand(sql, new { vals = new[] { 1, 2, 3, 4 } });
                int count = database.Query<int>(in command).Single();
                Assert.Equal(2, count);

                command = new PSqlCommand(sql, new { vals = new[] { 1 } });
                count = database.Query<int>(in command).Single();
                Assert.Equal(1, count);

                command = new PSqlCommand(sql, new { vals = new int[0] });
                count = database.Query<int>(in command).Single();
                Assert.Equal(0, count);
            }
        }

        [Fact(Skip = "Interface not implemented")]
        public void TestProcedureWithTimeParameter()
        {
    ////        using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
    ////        {
    ////            var p = new DynamicParameters();
    ////            p.Add("a", TimeSpan.FromHours(10), dbType: DbType.Time);

    ////            database.Execute($@"CREATE PROCEDURE #TestProcWithTimeParameter
    ////@a TIME
    ////AS 
    ////BEGIN
    ////SELECT @a
    ////END");
    ////            Assert.Equal(database.Query<TimeSpan>("#TestProcWithTimeParameter", p, commandType: CommandType.StoredProcedure).First(),
    ////                new TimeSpan(10, 0, 0));
    ////        }
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

                var command = new PSqlCommand("set @age = 11 select @name", p);
                Assert.Equal("bob", database.Query<string>(in command).First());
                Assert.Equal(11, p.Get<int>("age"));
            }
        }

        [Fact]
        public void TestSupportForDynamicParametersOutputExpressions()
        {
            using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
            {
                var bob = new Person { Name = "bob", PersonId = 1, Address = new Address { PersonId = 2 } };

                var p = new DynamicParameters(bob);
                p.Output(bob, b => b.PersonId);
                p.Output(bob, b => b.Occupation);
                p.Output(bob, b => b.NumberOfLegs);
                p.Output(bob, b => b.Address.Name);
                p.Output(bob, b => b.Address.PersonId);

                var command = new PSqlCommand(@"
SET @Occupation = 'grillmaster' 
SET @PersonId = @PersonId + 1 
SET @NumberOfLegs = @NumberOfLegs - 1
SET @AddressName = 'bobs burgers'
SET @AddressPersonId = @PersonId", p);
                database.Execute(in command);

                Assert.Equal("grillmaster", bob.Occupation);
                Assert.Equal(2, bob.PersonId);
                Assert.Equal(1, bob.NumberOfLegs);
                Assert.Equal("bobs burgers", bob.Address.Name);
                Assert.Equal(2, bob.Address.PersonId);
            }
        }

        [Fact]
        public void TestSupportForDynamicParametersOutputExpressions_Scalar()
        {
            using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
            {
                var bob = new Person { Name = "bob", PersonId = 1, Address = new Address { PersonId = 2 } };

                var p = new DynamicParameters(bob);
                p.Output(bob, b => b.PersonId);
                p.Output(bob, b => b.Occupation);
                p.Output(bob, b => b.NumberOfLegs);
                p.Output(bob, b => b.Address.Name);
                p.Output(bob, b => b.Address.PersonId);

                var command = new PSqlCommand(@"
SET @Occupation = 'grillmaster' 
SET @PersonId = @PersonId + 1 
SET @NumberOfLegs = @NumberOfLegs - 1
SET @AddressName = 'bobs burgers'
SET @AddressPersonId = @PersonId
select 42", p);
                var result = (int)database.ExecuteScalar<dynamic>(in command);

                Assert.Equal("grillmaster", bob.Occupation);
                Assert.Equal(2, bob.PersonId);
                Assert.Equal(1, bob.NumberOfLegs);
                Assert.Equal("bobs burgers", bob.Address.Name);
                Assert.Equal(2, bob.Address.PersonId);
                Assert.Equal(42, result);
            }
        }

        [Fact]
        public void TestSupportForDynamicParametersOutputExpressions_Query()
        {
            using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
            {
                var bob = new Person { Name = "bob", PersonId = 1, Address = new Address { PersonId = 2 } };

                var p = new DynamicParameters(bob);
                p.Output(bob, b => b.PersonId);
                p.Output(bob, b => b.Occupation);
                p.Output(bob, b => b.NumberOfLegs);
                p.Output(bob, b => b.Address.Name);
                p.Output(bob, b => b.Address.PersonId);

                var command = new PSqlCommand(@"
SET @Occupation = 'grillmaster' 
SET @PersonId = @PersonId + 1 
SET @NumberOfLegs = @NumberOfLegs - 1
SET @AddressName = 'bobs burgers'
SET @AddressPersonId = @PersonId
select 42", p);
                var result = database.Query<int>(in command).Single();

                Assert.Equal("grillmaster", bob.Occupation);
                Assert.Equal(2, bob.PersonId);
                Assert.Equal(1, bob.NumberOfLegs);
                Assert.Equal("bobs burgers", bob.Address.Name);
                Assert.Equal(2, bob.Address.PersonId);
                Assert.Equal(42, result);
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
                var command = new PSqlCommand("select @name", parameters);
                string result = database.Query<string>(in command).First();
                Assert.Equal("bob", result);
            }
        }

        public class HazX
        {
            public string X { get; set; }
        }

        [Fact]
        public void SO25297173_DynamicIn()
        {
            using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
            {
                const string query = @"
declare @table table(value int not null);
insert @table values(1);
insert @table values(2);
insert @table values(3);
insert @table values(4);
insert @table values(5);
insert @table values(6);
insert @table values(7);
SELECT value FROM @table WHERE value IN @myIds";
                var queryParams = new Dictionary<string, object>
                    {
                        ["myIds"] = new[] { 5, 6 }
                    };

                var dynamicParams = new DynamicParameters(queryParams);
                var command = new PSqlCommand(query, dynamicParams);
                var result = database.Query<int>(in command);
                Assert.Equal(2, result.Count);
                Assert.Contains(5, result);
                Assert.Contains(6, result);
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
                var command = new PSqlCommand("select @Foo", args);
                int i = database.Query<int>(in command).Single();
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
                var command = new PSqlCommand("select @Foo", args);
                int i = database.Query<int>(in command).Single();
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

                var command = new PSqlCommand("SELECT @param1", parameters);
                database.Query<dynamic>(in command);
            }
        }

        [Fact(Skip = "Interface not implemented")]
        public void TestParameterWithIndexer()
        {
            using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
            {
////                database.Execute($@"create proc #TestProcWithIndexer 
////	@A int
////as 
////begin
////	select @A
////end");
////                var item = database.Query<int>("#TestProcWithIndexer", new ParameterWithIndexer(), commandType: CommandType.StoredProcedure).Single();
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
                var command = new PSqlCommand("insert into #Dyno ([Id], [Name], [Foo]) values (@Id, @Name, @Foo);", orig);
                var result = database.Execute(in command);

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
        public void Issue151_ExpandoObjectArgsQuery()
        {
            using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
            {
                dynamic args = new ExpandoObject();
                args.Id = 123;
                args.Name = "abc";
                var command = new PSqlCommand("select @Id as [Id], @Name as [Name]", args);
                var row = database.Query<dynamic>(in command).Single();
                ((int)row.Id).Equals(123);
                ((string)row.Name).Equals("abc");
            }
        }

        [Fact]
        public void Issue151_ExpandoObjectArgsExec()
        {
            using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
            {
                dynamic args = new ExpandoObject();
                args.Id = 123;
                args.Name = "abc";
                database.Execute($"create table #issue151 (Id int not null, Name nvarchar(20) not null)");

                var command = new PSqlCommand("insert #issue151 values(@Id, @Name)", (object)args);

                Assert.Equal(1, database.Execute(in command).NumRowsAffected);
                var row = database.Query<dynamic>($"select Id, Name from #issue151").Single();
                ((int)row.Id).Equals(123);
                ((string)row.Name).Equals("abc");
            }
        }

        [Fact]
        public void Issue192_InParameterWorksWithSimilarNames()
        {
            using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
            {
                var command = new PSqlCommand(@"
declare @Issue192 table (
    Field INT NOT NULL PRIMARY KEY IDENTITY(1,1),
    Field_1 INT NOT NULL);
insert @Issue192(Field_1) values (1), (2), (3);
SELECT * FROM @Issue192 WHERE Field IN @Field AND Field_1 IN @Field_1",
                    new { Field = new[] { 1, 2 }, Field_1 = new[] { 2, 3 } });
                var rows = database.Query<dynamic>(in command).Single();
                Assert.Equal(2, (int)rows.Field);
                Assert.Equal(2, (int)rows.Field_1);
            }
        }

        [Fact]
        public void Issue192_InParameterWorksWithSimilarNamesWithUnicode()
        {
            using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
            {
                var command = new PSqlCommand(@"
declare @Issue192 table (
    Field INT NOT NULL PRIMARY KEY IDENTITY(1,1),
    Field_1 INT NOT NULL);
insert @Issue192(Field_1) values (1), (2), (3);
SELECT * FROM @Issue192 WHERE Field IN @µ AND Field_1 IN @µµ",
                    new { µ = new[] { 1, 2 }, µµ = new[] { 2, 3 } });
                var rows = database.Query<dynamic>(in command).Single();
                Assert.Equal(2, (int)rows.Field);
                Assert.Equal(2, (int)rows.Field_1);
            }
        }

        [Fact]
        public void Issue220_InParameterCanBeSpecifiedInAnyCase()
        {
            using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
            {
                // note this might fail if your database server is case-sensitive
                var command = new PSqlCommand("select * from (select 1 as Id) as X where Id in @ids", new { Ids = new[] { 1 } });
                Assert.Equal(new[] { 1 }, database.Query<int>(in command));
            }
        }

        [Fact]
        public void SO30156367_DynamicParamsWithoutExec()
        {
            using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
            {
                var dbParams = new DynamicParameters();
                dbParams.Add("Field1", 1);
                var value = dbParams.Get<int>("Field1");
                Assert.Equal(1, value);
            }
        }

        [Fact]
        public void RunAllStringSplitTestsDisabled()
        {
            this.RunAllStringSplitTests(-1, 1500);
        }

        private void RunAllStringSplitTests(int stringSplit, int max = 150)
        {
            using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
            {
                int oldVal = MapperSettings.InListStringSplitCount;
                try
                {
                    MapperSettings.InListStringSplitCount = stringSplit;
                    try
                    {
                        database.Execute($"drop table #splits");
                    }
                    catch
                    {
                        /* don't care */
                    }

                    var sqlCommand = new PSqlCommand("create table #splits (i int not null);"
                                                     + string.Concat(Enumerable
                                                                     .Range(-max, max * 3).Select(i => $"insert #splits (i) values ({i});"))
                                                     + "select count(1) from #splits");

                    int count = database.QuerySingle<int>(in sqlCommand);
                    Assert.Equal(count, 3 * max);

                    for (int i = 0; i < max; Incr(ref i))
                    {
                        try
                        {
                            var vals = Enumerable.Range(1, i);
                            var list = database.Query<int>($"select i from #splits where i in {vals}");
                            Assert.Equal(list.Count, i);
                            Assert.Equal(list.Sum(), vals.Sum());
                        }
                        catch (Exception ex)
                        {
                            throw new InvalidOperationException($"Error when i={i}: {ex.Message}", ex);
                        }
                    }
                }
                finally
                {
                    MapperSettings.InListStringSplitCount = oldVal;
                }

            }
        }

        private static void Incr(ref int i)
        {
            if (i <= 15) i++;
            else if (i <= 80) i += 5;
            else if (i <= 200) i += 10;
            else if (i <= 1000) i += 50;
            else i += 100;
        }

        [Fact]
        public void Issue601_InternationalParameterNamesWork()
        {
            using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
            {
                // regular parameter
                var command = new PSqlCommand("select @æøå٦", new { æøå٦ = 42 });
                var result = database.QuerySingle<int>(in command);
                Assert.Equal(42, result);
            }
        }

        [Fact(Skip = "Not working")]
        public void TestListExpansionPadding_Enabled() => this.TestListExpansionPadding(true);

        [Fact(Skip = "Not working")]
        public void TestListExpansionPadding_Disabled() => this.TestListExpansionPadding(false);

        private void TestListExpansionPadding(bool enabled)
        {
            using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
            {
                bool oldVal = MapperSettings.PadListExpansions;
                try
                {
                    MapperSettings.PadListExpansions = enabled;

                    var command = new PSqlCommand(@"
create table #ListExpansion(id int not null identity(1,1), value int null);
insert #ListExpansion (value) values (null);
declare @loop int = 0;
while (@loop < 12)
begin -- double it
	insert #ListExpansion (value) select value from #ListExpansion;
	set @loop = @loop + 1;
end

select count(1) as [Count] from #ListExpansion");

                    Assert.Equal(4096, database.ExecuteScalar<int>(in command));

                    var list = new List<int>();
                    int nextId = 1, batchCount;
                    var rand = new Random(12345);
                    const int SQL_SERVER_MAX_PARAMS = 2095;
                    this.TestListForExpansion(list, enabled); // test while empty
                    while (list.Count < SQL_SERVER_MAX_PARAMS)
                    {
                        try
                        {
                            if (list.Count <= 20) batchCount = 1;
                            else if (list.Count <= 200) batchCount = rand.Next(1, 40);
                            else batchCount = rand.Next(1, 100);

                            for (int j = 0; j < batchCount && list.Count < SQL_SERVER_MAX_PARAMS; j++)
                                list.Add(nextId++);

                            this.TestListForExpansion(list, enabled);
                        }
                        catch (Exception ex)
                        {
                            throw new InvalidOperationException($"Failure with {list.Count} items: {ex.Message}", ex);
                        }
                    }
                }
                finally
                {
                    MapperSettings.PadListExpansions = oldVal;
                }

            }
        }

        private void TestListForExpansion(List<int> list, bool enabled)
        {
            using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
            {
                var command = new PSqlCommand(@"
declare @hits int, @misses int, @count int;
select @count = count(1) from #ListExpansion;
select @hits = count(1) from #ListExpansion where id in @ids ;
select @misses = count(1) from #ListExpansion where not id in @ids ;
declare @query nvarchar(max) = N' in @ids '; -- ok, I confess to being pleased with this hack ;p
select @hits as [Hits], (@count - @misses) as [Misses], @query as [Query];
", new { ids = list });
                var row = database.QuerySingle<dynamic>(in command);
                int hits = row.Hits, misses = row.Misses;
                string query = row.Query;
                int argCount = Regex.Matches(query, "@ids[0-9]").Count;
                int expectedCount = GetExpectedListExpansionCount(list.Count, enabled);
                Assert.Equal(hits, list.Count);
                Assert.Equal(misses, list.Count);
                Assert.Equal(argCount, expectedCount);
            }
        }

        private static int GetExpectedListExpansionCount(int count, bool enabled)
        {
            if (!enabled) return count;

            if (count <= 5 || count > 2070) return count;

            int padFactor;
            if (count <= 150) padFactor = 10;
            else if (count <= 750) padFactor = 50;
            else if (count <= 2000) padFactor = 100;
            else if (count <= 2070) padFactor = 10;
            else padFactor = 200;

            int blocks = count / padFactor, delta = count % padFactor;
            if (delta != 0) blocks++;
            return blocks * padFactor;
        }
    }
}
