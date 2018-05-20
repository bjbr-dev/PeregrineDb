namespace PeregrineDb.Tests.Databases.Mapper.SharedTypes
{
    internal static class AbstractInheritance
    {
        public abstract class Order
        {
            public int Public { get; set; }
        }

        public class ConcreteOrder : Order
        {
            public int Concrete { get; set; }
        }
    }
}