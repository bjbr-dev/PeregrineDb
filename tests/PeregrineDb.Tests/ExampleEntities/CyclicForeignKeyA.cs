namespace PeregrineDb.Tests.ExampleEntities
{
    using System.ComponentModel.DataAnnotations.Schema;

    [Table("CyclicForeignKeyA")]
    public class CyclicForeignKeyA
    {
        public int Id { get; set; }

        public int? ForeignId { get; set; }
    }
}