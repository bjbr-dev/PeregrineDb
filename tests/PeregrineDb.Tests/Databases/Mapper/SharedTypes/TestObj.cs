namespace PeregrineDb.Tests.Databases.Mapper.SharedTypes
{
    internal class TestObj
    {
        public int _internal;

        internal int Internal
        {
            set { this._internal = value; }
        }

        public int _priv;

        private int Priv
        {
            set { this._priv = value; }
        }

        private int PrivGet => this._priv;
    }
}