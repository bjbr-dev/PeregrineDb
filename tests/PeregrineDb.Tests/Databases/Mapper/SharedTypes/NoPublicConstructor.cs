namespace PeregrineDb.Tests.Databases.Mapper.SharedTypes
{
    internal class NoPublicConstructor
    {
        private NoPublicConstructor()
        {
        }

        public int A { get; set; }
    }
}