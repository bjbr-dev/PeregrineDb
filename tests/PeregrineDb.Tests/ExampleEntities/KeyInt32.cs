namespace PeregrineDb.Tests.ExampleEntities
{
    using System.ComponentModel.DataAnnotations.Schema;

    [Table(nameof(KeyInt32))]
    public class KeyInt32
    {
        public long Id { get; set; }

        public string Name { get; set; }
    }
}