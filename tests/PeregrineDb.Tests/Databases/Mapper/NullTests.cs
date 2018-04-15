namespace PeregrineDb.Tests.Databases.Mapper
{
    using System.Linq;
    using PeregrineDb.Databases.Mapper;
    using PeregrineDb.Tests.Databases.Mapper.SharedTypes;
    using PeregrineDb.Tests.Utils;
    using Xunit;

    public class NullTests
    {
        [Fact]
        public void TestNullableDefault()
        {
            this.TestNullable(false);
        }

        [Fact]
        public void TestNullableApplyNulls()
        {
            this.TestNullable(true);
        }

        private void TestNullable(bool applyNulls)
        {
            using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
            {
                bool oldSetting = SqlMapper.Settings.ApplyNullValues;
                try
                {
                    SqlMapper.Settings.ApplyNullValues = applyNulls;
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
                    if (applyNulls)
                    {
                        Assert.Equal(2, obj.A); // cannot be null
                        Assert.Null(obj.B);
                        Assert.Null(obj.C);
                        Assert.Equal(AnEnum.B, obj.D);
                        Assert.Null(obj.E);
                    }
                    else
                    {
                        Assert.Equal(2, obj.A);
                        Assert.Equal(2, obj.B);
                        Assert.Equal("def", obj.C);
                        Assert.Equal(AnEnum.B, obj.D);
                        Assert.Equal(AnEnum.B, obj.E);
                    }
                }
                finally
                {
                    SqlMapper.Settings.ApplyNullValues = oldSetting;
                }
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
}
