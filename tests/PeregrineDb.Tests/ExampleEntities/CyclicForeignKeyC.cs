namespace PeregrineDb.Tests.ExampleEntities
{
    using System.ComponentModel.DataAnnotations.Schema;

    [Table("CyclicForeignKeyC")]
    public class CyclicForeignKeyC
    {
        public int Id { get; set; }

        public int? ForeignId { get; set; }
    }
}