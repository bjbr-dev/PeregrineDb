namespace PeregrineDb.Tests.ExampleEntities
{
    using System.ComponentModel.DataAnnotations;
    using System.ComponentModel.DataAnnotations.Schema;

    [Table(nameof(CompositeKeys))]
    public class CompositeKeys
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.None)]
        public int? Key1 { get; set; }

        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.None)]
        public int Key2 { get; set; }

        public string Name { get; set; }
    }
}