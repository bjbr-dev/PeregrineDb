namespace PeregrineDb.Tests.Databases
{
    using System;
    using System.Diagnostics.CodeAnalysis;
    using System.Linq;
    using FluentAssertions;
    using PeregrineDb.Dialects;
    using PeregrineDb.Tests.ExampleEntities;
    using PeregrineDb.Tests.Utils;
    using Xunit;

    public abstract partial class DefaultDatabaseConnectionCrudTests
    {
        public class Update
            : DefaultDatabaseConnectionCrudTests
        {
            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Updates_the_entity(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var id = database.Insert<int>(new Dog { Name = "Some name", Age = 10 });

                    // Act
                    var entity = database.Find<Dog>(id);
                    entity.Name = "Other name";
                    database.Update(entity);

                    // Assert
                    var updatedEntity = database.Find<Dog>(id);
                    updatedEntity.Name.Should().Be("Other name");
                }
            }

            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Ignores_columns_which_are_not_mapped(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var entity = new PropertyNotMapped { FirstName = "Bobby", LastName = "DropTables", Age = 10 };
                    entity.Id = database.Insert<int>(entity);

                    // Act
                    entity.LastName = "Other name";
                    database.Update(entity);

                    // Assert
                    var updatedEntity = database.Find<PropertyNotMapped>(entity.Id);
                    updatedEntity.LastName.Should().Be("Other name");
                }
            }

            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Updates_entities_with_composite_keys(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    var entity = new CompositeKeys { Key1 = 5, Key2 = 20, Name = "Some name" };
                    database.Insert(entity);

                    // Act
                    entity.Name = "Other name";
                    database.Update(entity);

                    // Assert
                    var id = new { Key1 = 5, Key2 = 20 };
                    var updatedEntity = database.Find<CompositeKeys>(id);

                    updatedEntity.Name.Should().Be("Other name");
                }
            }
        }

        public class UpdateRange
            : DefaultDatabaseConnectionCrudTests
        {
            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Updates_the_entity(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    database.InsertRange(
                        new[]
                            {
                                new Dog { Name = "Some name1", Age = 10 },
                                new Dog { Name = "Some name2", Age = 10 },
                                new Dog { Name = "Some name2", Age = 11 }
                            });

                    // Act
                    var entities = database.GetRange<Dog>($"WHERE Age = 10").ToList();
                    foreach (var entity in entities)
                    {
                        entity.Name = "Other name";
                    }


                    var result = database.UpdateRange(entities);

                    // Assert
                    result.NumRowsAffected.Should().Be(2);

                    var updatedEntities = database.GetRange<Dog>($"WHERE Name = 'Other name'");
                    updatedEntities.Count().Should().Be(2);
                }
            }


            [Theory]
            [MemberData(nameof(TestDialects))]
            public void Updates_entities_with_composite_keys(IDialect dialect)
            {
                using (var database = BlankDatabaseFactory.MakeDatabase(dialect))
                {
                    // Arrange
                    database.InsertRange(
                        new[]
                            {
                                new CompositeKeys { Key1 = 5, Key2 = 20, Name = "Some name1" },
                                new CompositeKeys { Key1 = 6, Key2 = 21, Name = "Some name2" },
                                new CompositeKeys { Key1 = 7, Key2 = 22, Name = "Some other name" }
                            });

                    // Act
                    var entities = database.GetRange<CompositeKeys>($"WHERE Name Like 'Some name%'").ToList();

                    foreach (var entity in entities)
                    {
                        entity.Name = "Other name";
                    }

                    var result = database.UpdateRange(entities);

                    // Assert
                    result.NumRowsAffected.Should().Be(2);

                    var updatedEntities = database.GetRange<CompositeKeys>($"WHERE Name = 'Other name'");
                    updatedEntities.Count().Should().Be(2);
                }
            }
        }
    }
}