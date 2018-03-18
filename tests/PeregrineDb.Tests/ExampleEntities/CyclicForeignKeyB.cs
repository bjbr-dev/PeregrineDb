namespace PeregrineDb.Tests.ExampleEntities
{
    using System.ComponentModel.DataAnnotations.Schema;

    [Table("CyclicForeignKeyB")]
    public class CyclicForeignKeyB
    {
        public int Id { get; set; }

        public int ForeignId { get; set; }
    }
}