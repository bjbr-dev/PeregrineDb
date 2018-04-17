namespace PeregrineDb.Databases.Mapper
{
    using System.Data;

    /// <typeparam name="T">The type to have a cache for.</typeparam>
    internal static class TypeHandlerCache<T>
    {
        private static ITypeHandler handler;

        /// <param name="value">The object to parse.</param>
        public static T Parse(object value) => (T)handler.Parse(typeof(T), value);

        /// <param name="parameter">The parameter to set a value for.</param>
        /// <param name="value">The value to set.</param>
        public static void SetValue(IDbDataParameter parameter, object value)
        {
            handler.SetValue(parameter, value);
        }

        internal static void SetHandler(ITypeHandler handler)
        {
            TypeHandlerCache<T>.handler = handler;
        }
    }
}