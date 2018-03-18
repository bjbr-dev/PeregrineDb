namespace PeregrineDb.Tests.ExampleEntities
{
    using System.ComponentModel.DataAnnotations.Schema;

    [Table(nameof(TempNoKey))]
    public class TempNoKey
    {
        public string Name { get; set; }

        public int Age { get; set; }
    }
}