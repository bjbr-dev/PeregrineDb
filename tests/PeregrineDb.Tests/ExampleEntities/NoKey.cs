namespace PeregrineDb.Tests.ExampleEntities
{
    using System.ComponentModel.DataAnnotations.Schema;

    [Table(nameof(NoKey))]
    public class NoKey
    {
        public string Name { get; set; }

        public int Age { get; set; }
    }
}