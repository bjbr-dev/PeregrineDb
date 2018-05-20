namespace PeregrineDb.Tests.Databases.Mapper.SharedTypes
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