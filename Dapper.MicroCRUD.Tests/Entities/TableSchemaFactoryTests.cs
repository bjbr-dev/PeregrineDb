// <copyright file="TableSchemaFactoryTests.cs" company="Berkeleybross">
// Copyright (c) Berkeleybross. All rights reserved.
// </copyright>
namespace Dapper.MicroCRUD.Tests.Entities
{
    using System;
    using System.ComponentModel.DataAnnotations;
    using System.ComponentModel.DataAnnotations.Schema;
    using System.Diagnostics.CodeAnalysis;
    using System.Linq;
    using Dapper.MicroCRUD.Entities;
    using NUnit.Framework;

    [TestFixture]
    [SuppressMessage("ReSharper", "UnusedAutoPropertyAccessor.Local")]
    [SuppressMessage("ReSharper", "MemberCanBePrivate.Local")]
    [SuppressMessage("ReSharper", "UnusedMember.Local")]
    [SuppressMessage("ReSharper", "UnassignedGetOnlyAutoProperty")]
    public class TableSchemaFactoryTests
    {
        private Dialect dialect;

        [SetUp]
        public void BaseSetUp()
        {
            this.dialect = new Dialect("TestDialect", "GET Identity();", "'{0}'", "LIMIT {0} OFFSET {1}");
        }

        private class GetTableSchema
            : TableSchemaFactoryTests
        {
            private TableSchema PerformAct(Type entityType)
            {
                return TableSchemaFactory.MakeTableSchema(entityType, this.dialect);
            }

            private class Naming
                : GetTableSchema
            {
                [Test]
                public void Uses_table_name_resolver_to_get_table_name()
                {
                    // Act
                    var result = this.PerformAct(typeof(SingleColumn));

                    // Assert
                    Assert.AreEqual("'SingleColumn'", result.Name);
                }

                [Test]
                public void Uses_column_name_resolver_to_get_column_name()
                {
                    // Act
                    var result = this.PerformAct(typeof(SingleColumn));

                    // Assert
                    var column = result.Columns.Single();
                    Assert.AreEqual("'Id'", column.ColumnName);
                }

                [Test]
                public void Uses_property_name_to_populate_select_name()
                {
                    // Act
                    var result = this.PerformAct(typeof(SingleColumn));

                    // Assert
                    var column = result.Columns.Single();
                    Assert.AreEqual(this.dialect.EscapeMostReservedCharacters("Id"), column.SelectName);
                }

                [Test]
                public void Uses_property_name_to_populate_parameter_name()
                {
                    // Act
                    var result = this.PerformAct(typeof(SingleColumn));

                    // Assert
                    var column = result.Columns.Single();
                    Assert.AreEqual("Id", column.ParameterName);
                }

                private class SingleColumn
                {
                    public int Id { get; set; }
                }
            }

            private class Columns
                : GetTableSchema
            {
                [Test]
                public void Treats_readonly_properties_as_computed()
                {
                    // Act
                    var result = this.PerformAct(typeof(ReadOnlyProperty));

                    // Assert
                    var column = result.Columns.Single();
                    Assert.IsFalse(column.Usage.IsPrimaryKey);
                    Assert.IsFalse(column.Usage.IncludeInInsertStatements);
                    Assert.IsFalse(column.Usage.IncludeInUpdateStatements);
                }

                [Test]
                public void Sets_none_generated_property_to_included_in_insert_statements()
                {
                    // Act
                    var result = this.PerformAct(typeof(PropertyNotGenerated));

                    // Assert
                    var column = result.Columns.Single();
                    Assert.IsTrue(column.Usage.IncludeInInsertStatements);
                }

                [Test]
                public void Sets_generated_property_to_not_be_in_insert_statements()
                {
                    // Act
                    var result = this.PerformAct(typeof(PropertyIdentity));

                    // Assert
                    var column = result.Columns.Single();
                    Assert.IsFalse(column.Usage.IncludeInInsertStatements);
                    Assert.IsTrue(column.Usage.IncludeInUpdateStatements);
                }

                [Test]
                public void Sets_computed_property_to_not_be_in_insert_or_update_statements()
                {
                    // Act
                    var result = this.PerformAct(typeof(PropertyComputed));

                    // Assert
                    var column = result.Columns.Single();
                    Assert.IsFalse(column.Usage.IncludeInInsertStatements);
                    Assert.IsFalse(column.Usage.IncludeInUpdateStatements);
                }

                [Test]
                public void Ignores_methods()
                {
                    // Act
                    var result = this.PerformAct(typeof(Method));

                    // Assert
                    Assert.IsEmpty(result.Columns);
                }

                [Test]
                public void Ignores_unmapped_properties()
                {
                    // Act
                    var result = this.PerformAct(typeof(NotMapped));

                    // Assert
                    Assert.IsEmpty(result.Columns);
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

            private class PrimaryKeys
                : GetTableSchema
            {
                [Test]
                public void Marks_property_called_id_as_primary_key()
                {
                    // Act
                    var result = this.PerformAct(typeof(KeyDefault));

                    // Assert
                    Assert.AreEqual("Id", result.PrimaryKeyColumns.Single().ParameterName);
                }

                [Test]
                public void Marks_property_with_key_attribute_as_primary_key()
                {
                    // Act
                    var result = this.PerformAct(typeof(KeyAlias));

                    // Assert
                    Assert.AreEqual("Key", result.PrimaryKeyColumns.Single().ParameterName);
                }

                [Test]
                public void Marks_properties_with_key_attribute_as_primary_keys()
                {
                    // Act
                    var result = this.PerformAct(typeof(KeyComposite));

                    // Assert
                    Assert.AreEqual(2, result.PrimaryKeyColumns.Length);
                }

                [Test]
                public void Takes_key_attribute_over_property_called_id()
                {
                    // Act
                    var result = this.PerformAct(typeof(KeyNotId));

                    // Assert
                    Assert.AreEqual("Key", result.PrimaryKeyColumns.Single().ParameterName);
                }

                [Test]
                public void Treats_readonly_keys_as_computed()
                {
                    // Act
                    var result = this.PerformAct(typeof(ReadOnlyKey));

                    // Assert
                    var column = result.Columns.Single();
                    Assert.IsTrue(column.Usage.IsPrimaryKey);
                    Assert.IsFalse(column.Usage.IncludeInInsertStatements);
                    Assert.IsFalse(column.Usage.IncludeInUpdateStatements);
                }

                [Test]
                public void Sets_none_generated_key_to_included_in_insert_statements()
                {
                    // Act
                    var result = this.PerformAct(typeof(KeyNotGenerated));

                    // Assert
                    var column = result.Columns.Single(c => c.ParameterName == "Id");
                    Assert.IsTrue(column.Usage.IncludeInInsertStatements);
                }

                [Test]
                public void Sets_computed_key_to_not_be_in_insert_or_update_statements()
                {
                    // Act
                    var result = this.PerformAct(typeof(KeyComputed));

                    // Assert
                    var column = result.Columns.Single(c => c.ParameterName == "Id");
                    Assert.IsFalse(column.Usage.IncludeInInsertStatements);
                    Assert.IsFalse(column.Usage.IncludeInUpdateStatements);
                }

                [Test]
                public void Sets_generated_key_to_not_be_in_insert_or_update_statements()
                {
                    // Act
                    var result = this.PerformAct(typeof(KeyIdentity));

                    // Assert
                    var column = result.Columns.Single(c => c.ParameterName == "Id");
                    Assert.IsFalse(column.Usage.IncludeInInsertStatements);
                    Assert.IsFalse(column.Usage.IncludeInUpdateStatements);
                }

                [Test]
                public void Ignores_methods()
                {
                    // Act
                    var result = this.PerformAct(typeof(MethodKey));

                    // Assert
                    Assert.IsEmpty(result.Columns);
                }

                [Test]
                public void Ignores_unmapped_properties()
                {
                    // Act
                    var result = this.PerformAct(typeof(NotMappedKey));

                    // Assert
                    Assert.IsEmpty(result.Columns);
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
            }
        }
    }
}