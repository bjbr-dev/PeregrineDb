namespace PeregrineDb.Tests.SharedTypes
{
    internal class MultipleConstructors
    {
        public MultipleConstructors()
        {
        }

        public MultipleConstructors(int a)
        {
            this.A = a + 1;
        }

        public int A { get; set; }
    }
}