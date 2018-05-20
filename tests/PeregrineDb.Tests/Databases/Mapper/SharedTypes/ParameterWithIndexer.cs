namespace PeregrineDb.Tests.Databases.Mapper.SharedTypes
{
    public class ParameterWithIndexer
    {
        public int A { get; set; }

        public virtual string this[string columnName]
        {
            get { return null; }
            set { }
        }
    }
}