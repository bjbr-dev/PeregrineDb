namespace PeregrineDb.Tests.Databases
{
    using System.Threading.Tasks;
    using PeregrineDb.Databases.Mapper;
    using PeregrineDb.Tests.Databases.Mapper.SharedTypes;
    using PeregrineDb.Tests.Utils;
    using Xunit;

    public abstract partial class DefaultDatabaseConnectionStatementsTests
    {
        public abstract class ExecuteScalarAsync
            : DefaultDatabaseConnectionStatementsTests
        {
            [Fact]
            public async Task Issue22_ExecuteScalarAsync()
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(Dialect.SqlServer2012))
                {
                    int i = await database.ExecuteScalarAsync<int>($"select 123").ConfigureAwait(false);
                    Assert.Equal(123, i);

                    i = await database.ExecuteScalarAsync<int>($"select cast(123 as bigint)").ConfigureAwait(false);
                    Assert.Equal(123, i);

                    long j = await database.ExecuteScalarAsync<long>($"select 123").ConfigureAwait(false);
                    Assert.Equal(123L, j);

                    j = await database.ExecuteScalarAsync<long>($"select cast(123 as bigint)").ConfigureAwait(false);
                    Assert.Equal(123L, j);

                    int? k = await database.ExecuteScalarAsync<int?>($"select {default(int?)}").ConfigureAwait(false);
                    Assert.Null(k);
                }
            }

            [Fact]
            public async Task TestSupportForDynamicParametersOutputExpressions_ScalarAsync()
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

                    var command = new SqlCommand($@"
SET @Occupation = 'grillmaster' 
SET @PersonId = @PersonId + 1 
SET @NumberOfLegs = @NumberOfLegs - 1
SET @AddressName = 'bobs burgers'
SET @AddressPersonId = @PersonId
select 42", p);
                    var result = (int)(await database.ExecuteScalarAsync<dynamic>(command).ConfigureAwait(false));

                    Assert.Equal("grillmaster", bob.Occupation);
                    Assert.Equal(2, bob.PersonId);
                    Assert.Equal(1, bob.NumberOfLegs);
                    Assert.Equal("bobs burgers", bob.Address.Name);
                    Assert.Equal(2, bob.Address.PersonId);
                    Assert.Equal(42, result);
                }
            }

        }
    }
}