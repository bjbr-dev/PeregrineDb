namespace PeregrineDb.Tests.ExampleEntities
{
    using System;
    using System.ComponentModel.DataAnnotations.Schema;

    [Table(nameof(PropertyComputed))]
    public class PropertyComputed
    {
        public int Id { get; set; }

        public string Name { get; set; }

        [DatabaseGenerated(DatabaseGeneratedOption.Computed)]
        public DateTime LastUpdated { get; set; }
    }
}