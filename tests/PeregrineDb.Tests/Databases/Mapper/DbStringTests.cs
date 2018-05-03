namespace PeregrineDb.Tests.Databases.Mapper
{
    using PeregrineDb.Mapping;
    using Xunit;

    public class DbStringTests
    {

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
    }
}
