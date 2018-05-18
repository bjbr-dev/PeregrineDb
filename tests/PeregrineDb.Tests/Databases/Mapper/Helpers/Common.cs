namespace PeregrineDb.Tests.Databases.Mapper.Helpers
{
    using System;
    using System.Data.Common;
    using PeregrineDb.Databases.Mapper;
    using PeregrineDb.Tests.Databases.Mapper.SharedTypes;
    using Xunit;
    using DynamicParameters = PeregrineDb.Mapping.DynamicParameters;

    public static class Common
    {
        public static Type GetSomeType() => typeof(SomeType);

        public static void DapperEnumValue(ISqlConnection connection)
        {
            // test passing as AsEnum, reading as int
            var v = (AnEnum)connection.QuerySingle<int>($"select {AnEnum.B}, {(AnEnum?)AnEnum.B}, {(AnEnum?)null}");
            Assert.Equal(AnEnum.B, v);

            var args = new DynamicParameters();
            args.Add("v", AnEnum.B);
            args.Add("y", AnEnum.B);
            args.Add("z", null);
            v = (AnEnum)connection.RawQuerySingle<int>("select @v, @y, @z", args);
            Assert.Equal(AnEnum.B, v);

            // test passing as int, reading as AnEnum
            var k = (int)connection.QuerySingle<AnEnum>($"select {(int)AnEnum.B}, {(int?)(int)AnEnum.B}, {(int?)null}");
            Assert.Equal(k, (int)AnEnum.B);

            args = new DynamicParameters();
            args.Add("v", (int)AnEnum.B);
            args.Add("y", (int)AnEnum.B);
            args.Add("z", null);
            k = (int)connection.RawQuerySingle<AnEnum>("select @v, @y, @z", args);
            Assert.Equal(k, (int)AnEnum.B);
        }

        public static void TestDateTime(ISqlConnection connection)
        {
            DateTime? now = DateTime.UtcNow;
            try { connection.Execute($"DROP TABLE Persons"); } catch { /* don't care */ }
            connection.Execute($"CREATE TABLE Persons (id int not null, dob datetime null)");
            connection.Execute($"INSERT Persons (id, dob) values ({7}, {(DateTime?)null})");
            connection.Execute($"INSERT Persons (id, dob) values ({42}, {now})");

            var row = connection.QueryFirstOrDefault<NullableDatePerson>($"SELECT id, dob, dob as dob2 FROM Persons WHERE id={7}");
            Assert.NotNull(row);
            Assert.Equal(7, row.Id);
            Assert.Null(row.DoB);
            Assert.Null(row.DoB2);

            row = connection.QueryFirstOrDefault<NullableDatePerson>($"SELECT id, dob FROM Persons WHERE id={42}");
            Assert.NotNull(row);
            Assert.Equal(42, row.Id);
            row.DoB.Equals(now);
            row.DoB2.Equals(now);
        }

        private class NullableDatePerson
        {
            public int Id { get; set; }
            public DateTime? DoB { get; set; }
            public DateTime? DoB2 { get; set; }
        }
    }
}
