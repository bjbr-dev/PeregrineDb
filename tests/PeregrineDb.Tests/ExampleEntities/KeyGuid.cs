namespace PeregrineDb.Tests.ExampleEntities
{
    using System;
    using System.ComponentModel.DataAnnotations.Schema;

    [Table(nameof(KeyGuid))]
    public class KeyGuid
    {
        [DatabaseGenerated(DatabaseGeneratedOption.None)]
        public Guid Id { get; set; }

        public string Name { get; set; }
    }
}