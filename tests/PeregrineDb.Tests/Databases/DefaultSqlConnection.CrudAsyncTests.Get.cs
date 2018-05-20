namespace PeregrineDb.Tests.Databases
{
    using System;
    using System.Diagnostics.CodeAnalysis;
    using System.Threading.Tasks;
    using FluentAssertions;
    using PeregrineDb.Dialects;
    using PeregrineDb.Schema;
    using PeregrineDb.Tests.ExampleEntities;
    using PeregrineDb.Tests.Utils;
    using Xunit;

    [SuppressMessage("ReSharper", "StringLiteralAsInterpolationArgument")]
    [SuppressMessage("ReSharper", "AccessToDisposedClosure")]
    public abstract partial class DefaultDatabaseConnectionCrudAsyncTests
    {
        public class GetAsync
            : DefaultDatabaseConnectionCrudAsyncTests
        {
            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Throws_exception_when_entity_has_no_key(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    database.Insert(new NoKey { Name = "Some Name", Age = 1 });

                    // Act
                    Func<Task> act = async () => await database.GetAsync<NoKey>("Some Name");

                    // Assert
                    act.ShouldThrow<InvalidPrimaryKeyException>();
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Throws_exception_when_entity_is_not_found(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    Func<Task> act = async () => await database.GetAsync<KeyInt32>(5);

                    // Assert
                    act.ShouldThrow<InvalidOperationException>();
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Finds_entity_by_Int32_primary_key(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var id = database.Insert<int>(new KeyInt32 { Name = "Some Name" });

                    // Act
                    var entity = await database.GetAsync<KeyInt32>(id);

                    // Assert
                    entity.Name.Should().Be("Some Name");
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Finds_entity_by_Int64_primary_key(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var id = database.Insert<long>(new KeyInt64 { Name = "Some Name" });

                    // Act
                    var entity = await database.GetAsync<KeyInt64>(id);

                    // Assert
                    entity.Name.Should().Be("Some Name");
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Finds_entity_by_string_primary_key(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    database.Insert(new KeyString { Name = "Some Name", Age = 42 });

                    // Act
                    var entity = await database.GetAsync<KeyString>("Some Name");

                    // Assert
                    entity.Age.Should().Be(42);
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Finds_entity_by_guid_primary_key(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var id = Guid.NewGuid();
                    database.Insert(new KeyGuid { Id = id, Name = "Some Name" });

                    // Act
                    var entity = await database.GetAsync<KeyGuid>(id);

                    // Assert
                    entity.Name.Should().Be("Some Name");
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Finds_entity_by_composite_key(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    database.Insert(new CompositeKeys { Key1 = 1, Key2 = 1, Name = "Some Name" });
                    var id = new { Key1 = 1, Key2 = 1 };

                    // Act
                    var entity = await database.GetAsync<CompositeKeys>(id);

                    // Assert
                    entity.Name.Should().Be("Some Name");
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Finds_entities_in_alternate_schema(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var id = database.Insert<int>(new SchemaOther { Name = "Some Name" });

                    // Act
                    var entity = await database.GetAsync<SchemaOther>(id);

                    // Assert
                    entity.Name.Should().Be("Some Name");
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public async Task Ignores_columns_which_are_not_mapped(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var id = database.Insert<int>(new PropertyNotMapped { FirstName = "Bobby", LastName = "DropTables", Age = 10 });

                    // Act
                    var entity = await database.GetAsync<PropertyNotMapped>(id);

                    // Assert
                    entity.FirstName.Should().Be("Bobby");
                    entity.LastName.Should().Be("DropTables");
                    entity.FullName.Should().Be("Bobby DropTables");
                    entity.Age.Should().Be(0);
                }
            }
        }
    }
}