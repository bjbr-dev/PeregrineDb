namespace PeregrineDb.Tests.ExampleEntities
{
    using System;
    using System.ComponentModel.DataAnnotations.Schema;

    [Table(nameof(PropertyGenerated))]
    public class PropertyGenerated
    {
        public int Id { get; set; }

        public string Name { get; set; }

        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public DateTime Created { get; set; }
    }
}