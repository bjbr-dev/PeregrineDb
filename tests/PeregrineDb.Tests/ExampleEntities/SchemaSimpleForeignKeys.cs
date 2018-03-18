namespace PeregrineDb.Tests.ExampleEntities
{
    using System.ComponentModel.DataAnnotations.Schema;

    [Table(nameof(SchemaSimpleForeignKeys), Schema = "Other")]
    public class SchemaSimpleForeignKeys
    {
        public int Id { get; set; }

        public int SchemaOtherId { get; set; }
    }
}