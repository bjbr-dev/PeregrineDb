namespace PeregrineDb.Tests.ExampleEntities
{
    using System.ComponentModel.DataAnnotations.Schema;

    [Table(nameof(KeyInt64))]
    public class KeyInt64
    {
        public long Id { get; set; }

        public string Name { get; set; }
    }
}