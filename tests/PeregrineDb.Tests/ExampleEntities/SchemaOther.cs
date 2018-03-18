namespace PeregrineDb.Tests.ExampleEntities
{
    using System.ComponentModel.DataAnnotations.Schema;

    [Table(nameof(SchemaOther), Schema = "Other")]
    public class SchemaOther
    {
        public int Id { get; set; }

        public string Name { get; set; }
    }
}