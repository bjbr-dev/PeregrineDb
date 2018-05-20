namespace PeregrineDb.Tests.SharedTypes
{
    internal class NoParameterlessConstructor
    {
        private NoParameterlessConstructor(int a)
        {
            this.A = a;
        }

        public int A { get; set; }
    }
}