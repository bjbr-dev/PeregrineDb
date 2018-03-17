// <copyright file="TableSchemaFactoryTests.cs" company="Berkeleybross">
// Copyright (c) Berkeleybross. All rights reserved.
// </copyright>
namespace Dapper.MicroCRUD.Tests.Schema
{
    using System;
    using System.Collections.Immutable;
    using System.ComponentModel.DataAnnotations;
    using System.ComponentModel.DataAnnotations.Schema;
    using System.Diagnostics.CodeAnalysis;
    using System.Linq;
    using Dapper.MicroCRUD.Dialects;
    using Dapper.MicroCRUD.Schema;
    using Dapper.MicroCRUD.Tests.ExampleEntities;
    using Dapper.MicroCRUD.Tests.Utils;
    using FluentAssertions;
    using Moq;
    using Xunit;

    [SuppressMessage("ReSharper", "UnusedAutoPropertyAccessor.Local")]
    [SuppressMessage("ReSharper", "MemberCanBePrivate.Local")]
    [SuppressMessage("ReSharper", "UnusedMember.Local")]
    [SuppressMessage("ReSharper", "UnassignedGetOnlyAutoProperty")]
    public class TableSchemaFactoryTests
    {
        private readonly IDialect dialect = new TestDialect();
        private TableSchemaFactory sut = new TableSchemaFactory(new DefaultTableNameFactory(), new DefaultColumnNameFactory());

        public class MakeTableSchema
            : TableSchemaFactoryTests
        {
            private TableSchema PerformAct(Type entityType)
            {
                return this.sut.MakeTableSchema(entityType, this.dialect);
            }

            public class Naming
                : MakeTableSchema
            {
                [Fact]
                public void Uses_table_name_resolver_to_get_table_name()
                {
                    // Arrange
                    var tableNameFactory = new Mock<ITableNameFactory>();
                    tableNameFactory.Setup(f => f.GetTableName(typeof(SingleColumn), this.dialect))
                                    .Returns("'SingleColumn'");
                    this.sut = this.sut.WithTableNameFactory(tableNameFactory.Object);

                    // Act
                    var result = this.PerformAct(typeof(SingleColumn));

                    // Assert
                    result.Name.Should().Be("'SingleColumn'");
                }

                [Fact]
                public void Uses_column_name_resolver_to_get_column_name()
                {
                    // Arrange
                    var columnNameFactory = new Mock<IColumnNameFactory>();
                    columnNameFactory.Setup(
                                         f => f.GetColumnName(
                                             It.Is<PropertySchema>(p => p.Name == "Id")))
                                     .Returns("Id");

                    this.sut = this.sut.WithColumnNameFactory(columnNameFactory.Object);

                    // Act
                    var result = this.PerformAct(typeof(SingleColumn));

                    // Assert
                    var column = result.Columns.Single();
                    column.ColumnName.Should().Be("'Id'");
                }

                [Fact]
                public void Uses_property_name_to_populate_select_name()
                {
                    // Act
                    var result = this.PerformAct(typeof(SingleColumn));

                    // Assert
                    var column = result.Columns.Single();
                    column.SelectName.Should().Be("'Id'");
                }

                [Fact]
                public void Uses_property_name_to_populate_parameter_name()
                {
                    // Act
                    var result = this.PerformAct(typeof(SingleColumn));

                    // Assert
                    var column = result.Columns.Single();
                    column.ParameterName.Should().Be("Id");
                }

                private class SingleColumn
                {
                    public int Id { get; set; }
                }
            }

            public class Columns
                : MakeTableSchema
            {
                [Fact]
                public void Treats_readonly_properties_as_computed()
                {
                    // Act
                    var result = this.PerformAct(typeof(ReadOnlyProperty));

                    // Assert
                    var column = result.Columns.Single();
                    column.Usage.IsPrimaryKey.Should().BeFalse();
                    column.Usage.IncludeInInsertStatements.Should().BeFalse();
                    column.Usage.IncludeInUpdateStatements.Should().BeFalse();
                }

                [Fact]
                public void Sets_none_generated_property_to_included_in_insert_statements()
                {
                    // Act
                    var result = this.PerformAct(typeof(PropertyNotGenerated));

                    // Assert
                    var column = result.Columns.Single();
                    column.Usage.IncludeInInsertStatements.Should().BeTrue();
                }

                [Fact]
                public void Sets_generated_property_to_not_be_in_insert_statements()
                {
                    // Act
                    var result = this.PerformAct(typeof(PropertyIdentity));

                    // Assert
                    var column = result.Columns.Single();
                    column.Usage.IncludeInInsertStatements.Should().BeFalse();
                    column.Usage.IncludeInUpdateStatements.Should().BeTrue();
                }

                [Fact]
                public void Sets_computed_property_to_not_be_in_insert_or_update_statements()
                {
                    // Act
                    var result = this.PerformAct(typeof(PropertyComputed));

                    // Assert
                    var column = result.Columns.Single();
                    column.Usage.IncludeInInsertStatements.Should().BeFalse();
                    column.Usage.IncludeInUpdateStatements.Should().BeFalse();
                }

                [Fact]
                public void Ignores_methods()
                {
                    // Act
                    var result = this.PerformAct(typeof(Method));

                    // Assert
                    result.Columns.Should().BeEmpty();
                }

                [Fact]
                public void Ignores_unmapped_properties()
                {
                    // Act
                    var result = this.PerformAct(typeof(NotMapped));

                    // Assert
                    result.Columns.Should().BeEmpty();
                }

                private class ReadOnlyProperty
                {
                    public DateTime LastUpdated { get; }
                }

                private class PropertyNotGenerated
                {
                    [DatabaseGenerated(DatabaseGeneratedOption.None)]
                    public int Name { get; set; }
                }

                private class PropertyComputed
                {
                    [DatabaseGenerated(DatabaseGeneratedOption.Computed)]
                    public int Name { get; set; }
                }

                private class PropertyIdentity
                {
                    [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
                    public int Name { get; set; }
                }

                private class Method
                {
                    public string Value(string value)
                    {
                        throw new NotImplementedException();
                    }
                }

                private class NotMapped
                {
                    [NotMapped]
                    public string Value { get; set; }
                }
            }

            public class PrimaryKeys
                : MakeTableSchema
            {
                [Fact]
                public void Marks_property_called_id_as_primary_key()
                {
                    // Act
                    var result = this.PerformAct(typeof(KeyDefault));

                    // Assert
                    result.PrimaryKeyColumns.Single().ParameterName.Should().Be("Id");
                }

                [Fact]
                public void Marks_property_with_key_attribute_as_primary_key()
                {
                    // Act
                    var result = this.PerformAct(typeof(KeyAlias));

                    // Assert
                    result.PrimaryKeyColumns.Single().ParameterName.Should().Be("Key");
                }

                [Fact]
                public void Marks_properties_with_key_attribute_as_primary_keys()
                {
                    // Act
                    var result = this.PerformAct(typeof(KeyComposite));

                    // Assert
                    result.PrimaryKeyColumns.Length.Should().Be(2);
                }

                [Fact]
                public void Takes_key_attribute_over_property_called_id()
                {
                    // Act
                    var result = this.PerformAct(typeof(KeyNotId));

                    // Assert
                    result.PrimaryKeyColumns.Single().ParameterName.Should().Be("Key");
                }

                [Fact]
                public void Treats_readonly_keys_as_computed()
                {
                    // Act
                    var result = this.PerformAct(typeof(ReadOnlyKey));

                    // Assert
                    var column = result.Columns.Single();
                    column.Usage.IsPrimaryKey.Should().BeTrue();
                    column.Usage.IncludeInInsertStatements.Should().BeFalse();
                    column.Usage.IncludeInUpdateStatements.Should().BeFalse();
                }

                [Fact]
                public void Sets_none_generated_key_to_included_in_insert_statements()
                {
                    // Act
                    var result = this.PerformAct(typeof(KeyNotGenerated));

                    // Assert
                    var column = result.Columns.Single(c => c.ParameterName == "Id");
                    column.Usage.IncludeInInsertStatements.Should().BeTrue();
                }

                [Fact]
                public void Sets_computed_key_to_not_be_in_insert_or_update_statements()
                {
                    // Act
                    var result = this.PerformAct(typeof(KeyComputed));

                    // Assert
                    var column = result.Columns.Single(c => c.ParameterName == "Id");
                    column.Usage.IncludeInInsertStatements.Should().BeFalse();
                    column.Usage.IncludeInUpdateStatements.Should().BeFalse();
                }

                [Fact]
                public void Sets_generated_key_to_not_be_in_insert_or_update_statements()
                {
                    // Act
                    var result = this.PerformAct(typeof(KeyIdentity));

                    // Assert
                    var column = result.Columns.Single(c => c.ParameterName == "Id");
                    column.Usage.IncludeInInsertStatements.Should().BeFalse();
                    column.Usage.IncludeInUpdateStatements.Should().BeFalse();
                }

                [Fact]
                public void Ignores_methods()
                {
                    // Act
                    var result = this.PerformAct(typeof(MethodKey));

                    // Assert
                    result.Columns.Should().BeEmpty();
                }

                [Fact]
                public void Ignores_unmapped_properties()
                {
                    // Act
                    var result = this.PerformAct(typeof(NotMappedKey));

                    // Assert
                    result.Columns.Should().BeEmpty();
                }

                [Fact]
                public void Ignores_static_properties()
                {
                    // Act
                    var result = this.PerformAct(typeof(StaticProperty));

                    // Assert
                    result.Columns.Should().BeEmpty();
                }

                [Fact]
                public void Ignores_indexers()
                {
                    // Act
                    var result = this.PerformAct(typeof(Indexer));

                    // Assert
                    result.Columns.Should().BeEmpty();
                }

                private class ReadOnlyKey
                {
                    public int Id { get; }
                }

                private class KeyDefault
                {
                    public int Id { get; set; }
                }

                private class KeyAlias
                {
                    [Key]
                    public int Key { get; set; }
                }

                private class KeyComposite
                {
                    [Key]
                    public int Key1 { get; set; }

                    [Key]
                    public int Key2 { get; set; }
                }

                private class KeyNotId
                {
                    public int Id { get; set; }

                    [Key]
                    public int Key { get; set; }
                }

                private class KeyNotGenerated
                {
                    [DatabaseGenerated(DatabaseGeneratedOption.None)]
                    public int Id { get; set; }
                }

                private class KeyComputed
                {
                    [DatabaseGenerated(DatabaseGeneratedOption.Computed)]
                    public int Id { get; set; }
                }

                private class KeyIdentity
                {
                    [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
                    public int Id { get; set; }
                }

                private class MethodKey
                {
                    public int Id()
                    {
                        throw new NotImplementedException();
                    }
                }

                private class NotMappedKey
                {
                    [NotMapped]
                    public int Id { get; set; }
                }

                private class StaticProperty
                {
                    public static string Name { get; set; }
                }

                private class Indexer
                {
                    public string this[int i]
                    {
                        get { throw new NotImplementedException(); }
                        set { throw new NotImplementedException(); }
                    }
                }
            }
        }

        public class MakeConditionsSchema
            : TableSchemaFactoryTests
        {
            private ImmutableArray<ConditionColumnSchema> PerformAct<T>(T conditions, TableSchema schema)
            {
                return this.sut.MakeConditionsSchema(typeof(T), schema);
            }

            private TableSchema GetTableSchema<T>()
            {
                return this.sut.MakeTableSchema(typeof(T), this.dialect);
            }

            public class Naming
                : MakeConditionsSchema
            {
                [Fact]
                public void Gets_column_by_property_name()
                {
                    // Arrange
                    var tableSchema = this.GetTableSchema<PropertyAlias>();

                    // Act
                    var result = this.PerformAct(new { Age = 12 }, tableSchema);

                    // Assert
                    var column = result.Single().Column;
                    column.ColumnName.Should().Be("'YearsOld'");
                    column.ParameterName.Should().Be("Age");
                }
            }

            public class Columns
                : MakeConditionsSchema
            {
                [Fact]
                public void Ignores_methods()
                {
                    // Arrange
                    var tableSchema = this.GetTableSchema<Method>();

                    // Act
                    var result = this.PerformAct(new Method(), tableSchema);

                    // Assert
                    result.Should().BeEmpty();
                }

                [Fact]
                public void Ignores_unmapped_properties()
                {
                    // Arrange
                    var tableSchema = this.GetTableSchema<NotMapped>();

                    // Act
                    var result = this.PerformAct(new NotMapped(), tableSchema);

                    // Assert
                    result.Should().BeEmpty();
                }

                [Fact]
                public void Ignores_unreadable_properties()
                {
                    // Arrange
                    var tableSchema = this.GetTableSchema<NotMapped>();

                    // Act
                    var result = this.PerformAct(new NotMapped(), tableSchema);

                    // Assert
                    result.Should().BeEmpty();
                }

                private class Method
                {
                    public string Value(string value)
                    {
                        throw new NotImplementedException();
                    }
                }

                private class NotMapped
                {
                    [NotMapped]
                    public string Value { get; set; }
                }

                private class PropertyNotReadable
                {
                    private string name;

                    public string Name
                    {
                        set { this.name = value; }
                    }
                }
            }
        }
    }
}