namespace PeregrineDb.Databases
{
    using System;
    using System.Data;
    using System.Globalization;
    using PeregrineDb.Databases.Mapper;
    using PeregrineDb.Mapping;
    using PeregrineDb.Utils;

    internal static class DataReaderExtensions
    {
        public static Func<IDataReader, T> MakeDeserializer<T>(this IDataReader reader, CacheInfo info, Type effectiveType, Identity identity)
        {
            var tuple = info.Deserializer;
            var hash = SqlMapper.GetColumnHash(reader);
            if (tuple.Func == null || tuple.Hash != hash)
            {
                tuple = info.Deserializer = new DeserializerState(hash, TypeMapper.GetDeserializer(effectiveType, reader));
                QueryCache.SetQueryCache(identity, info);
            }

            var convertToType = effectiveType.GetUnderlyingType();
            return r => Deserialze<T>(r, tuple, convertToType);
        }

        private static T Deserialze<T>(IDataReader reader, DeserializerState tuple, Type effectiveType)
        {
            var val = tuple.Func(reader);
            if (val == null || val is T)
            {
                return (T)val;
            }

            return (T)Convert.ChangeType(val, effectiveType, CultureInfo.InvariantCulture);
        }
    }
}