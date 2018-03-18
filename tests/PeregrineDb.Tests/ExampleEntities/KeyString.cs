namespace PeregrineDb.Tests.ExampleEntities
{
    using System.ComponentModel.DataAnnotations;
    using System.ComponentModel.DataAnnotations.Schema;

    [Table(nameof(KeyString))]
    public class KeyString
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.None)]
        public string Name { get; set; }

        public int Age { get; set; }
    }
}