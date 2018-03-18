namespace PeregrineDb.Tests.ExampleEntities
{
    using System.ComponentModel.DataAnnotations.Schema;

    [Table(nameof(PropertyNotMapped))]
    public class PropertyNotMapped
    {
        public int Id { get; set; }

        public string Firstname { get; set; }

        public string LastName { get; set; }

        [NotMapped]
        public int Age { get; set; }

        [NotMapped]
        public string FullName => this.Firstname + " " + this.LastName;
    }
}