namespace Dapper.MicroCRUD.Tests.ExampleEntities
{
    public class SelfReferenceForeignKey
    {
        public int Id { get; set; }

        public int? ForeignId { get; set; }
    }
}