namespace PeregrineDb.Tests.SharedTypes
{
    internal class TestFieldsEntity
    {
        public int a;

        public int b = 5;

        private int c;

        public int GetC()
        {
            return this.c;
        }
    }
}