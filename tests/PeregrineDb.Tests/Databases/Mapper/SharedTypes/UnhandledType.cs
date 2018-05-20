namespace PeregrineDb.Tests.Databases.Mapper.SharedTypes
{
    public enum UnhandledTypeOptions
    {
        Default
    }

    public class UnhandledType
    {
        private readonly UnhandledTypeOptions options;

        public UnhandledType(UnhandledTypeOptions options)
        {
            this.options = options;
        }
    }
}
