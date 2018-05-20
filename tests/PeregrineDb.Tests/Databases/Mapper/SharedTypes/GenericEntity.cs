namespace PeregrineDb.Tests.Databases.Mapper.SharedTypes
{
    public class GenericEntity
    {
        public static GenericEntity<T> From<T>(T value)
        {
            return new GenericEntity<T>(value);
        }
    }
}