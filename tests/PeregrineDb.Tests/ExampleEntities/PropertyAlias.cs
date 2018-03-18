namespace PeregrineDb.Tests.ExampleEntities
{
    using System.ComponentModel.DataAnnotations.Schema;

    [Table(nameof(PropertyAlias))]
    public class PropertyAlias
    {
        public int Id { get; set; }

        [Column("YearsOld")]
        public int Age { get; set; }
    }
}
