namespace PeregrineDb.Tests.SharedTypes
{
    internal class NoPublicConstructor
    {
        private NoPublicConstructor()
        {
        }

        public int A { get; set; }
    }
}