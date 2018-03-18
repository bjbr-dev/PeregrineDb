namespace PeregrineDb.Tests.ExampleEntities
{
    using System.ComponentModel.DataAnnotations.Schema;

    [Table(nameof(PropertyEnum))]
    public class PropertyEnum
    {
        public int Id { get; set; }

        public Color? FavoriteColor { get; set; }
    }
}