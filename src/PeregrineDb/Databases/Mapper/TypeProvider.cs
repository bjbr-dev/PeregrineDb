namespace PeregrineDb.Databases.Mapper
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Data;
    using System.Reflection;
    using PeregrineDb.Mapping;

    public static class TypeProvider
    {
        internal const string LinqBinary = "System.Data.Linq.Binary";
        private static Dictionary<Type, DbType> typeMap;
        private static Dictionary<Type, IDbTypeConverter> typeHandlers;

        static TypeProvider()
        {
            typeMap = new Dictionary<Type, DbType>
            {
                [typeof(byte)] = DbType.Byte,
                [typeof(sbyte)] = DbType.SByte,
                [typeof(short)] = DbType.Int16,
                [typeof(ushort)] = DbType.UInt16,
                [typeof(int)] = DbType.Int32,
                [typeof(uint)] = DbType.UInt32,
                [typeof(long)] = DbType.Int64,
                [typeof(ulong)] = DbType.UInt64,
                [typeof(float)] = DbType.Single,
                [typeof(double)] = DbType.Double,
                [typeof(decimal)] = DbType.Decimal,
                [typeof(bool)] = DbType.Boolean,
                [typeof(string)] = DbType.String,
                [typeof(char)] = DbType.StringFixedLength,
                [typeof(Guid)] = DbType.Guid,
                [typeof(DateTime)] = DbType.DateTime,
                [typeof(DateTimeOffset)] = DbType.DateTimeOffset,
                [typeof(TimeSpan)] = DbType.Time,
                [typeof(byte[])] = DbType.Binary,
                [typeof(byte?)] = DbType.Byte,
                [typeof(sbyte?)] = DbType.SByte,
                [typeof(short?)] = DbType.Int16,
                [typeof(ushort?)] = DbType.UInt16,
                [typeof(int?)] = DbType.Int32,
                [typeof(uint?)] = DbType.UInt32,
                [typeof(long?)] = DbType.Int64,
                [typeof(ulong?)] = DbType.UInt64,
                [typeof(float?)] = DbType.Single,
                [typeof(double?)] = DbType.Double,
                [typeof(decimal?)] = DbType.Decimal,
                [typeof(bool?)] = DbType.Boolean,
                [typeof(char?)] = DbType.StringFixedLength,
                [typeof(Guid?)] = DbType.Guid,
                [typeof(DateTime?)] = DbType.DateTime,
                [typeof(DateTimeOffset?)] = DbType.DateTimeOffset,
                [typeof(TimeSpan?)] = DbType.Time,
                [typeof(object)] = DbType.Object
            };
            ResetTypeHandlers();
        }

        /// <summary>
        /// Clear the registered type handlers.
        /// </summary>
        public static void ResetTypeHandlers()
        {
            typeHandlers = new Dictionary<Type, IDbTypeConverter>();
        }

        /// <summary>
        /// Configure the specified type to be mapped to a given db-type.
        /// </summary>
        /// <param name="type">The type to map from.</param>
        /// <param name="dbType">The database type to map to.</param>
        public static void AddTypeMap(Type type, DbType dbType)
        {
            // use clone, mutate, replace to avoid threading issues
            var snapshot = typeMap;

            if (snapshot.TryGetValue(type, out var oldValue) && oldValue == dbType) return; // nothing to do

            typeMap = new Dictionary<Type, DbType>(snapshot) { [type] = dbType };
        }

        /// <summary>
        /// Removes the specified type from the Type/DbType mapping table.
        /// </summary>
        /// <param name="type">The type to remove from the current map.</param>
        public static void RemoveTypeMap(Type type)
        {
            // use clone, mutate, replace to avoid threading issues
            var snapshot = typeMap;

            if (!snapshot.ContainsKey(type)) return; // nothing to do

            var newCopy = new Dictionary<Type, DbType>(snapshot);
            newCopy.Remove(type);

            typeMap = newCopy;
        }

        /// <summary>
        /// Configure the specified type to be processed by a custom handler.
        /// </summary>
        /// <param name="type">The type to handle.</param>
        /// <param name="converter">The handler to process the <paramref name="type"/>.</param>
        public static void AddTypeHandler(Type type, IDbTypeConverter converter) => AddTypeHandlerImpl(type, converter, true);

        internal static bool HasTypeHandler(Type type) => typeHandlers.ContainsKey(type);

        /// <summary>
        /// Configure the specified type to be processed by a custom handler.
        /// </summary>
        /// <param name="type">The type to handle.</param>
        /// <param name="converter">The handler to process the <paramref name="type"/>.</param>
        /// <param name="clone">Whether to clone the current type handler map.</param>
        private static void AddTypeHandlerImpl(Type type, IDbTypeConverter converter, bool clone)
        {
            if (type == null) throw new ArgumentNullException(nameof(type));

            Type secondary = null;
            if (type.IsValueType())
            {
                var underlying = Nullable.GetUnderlyingType(type);
                if (underlying == null)
                {
                    secondary = typeof(Nullable<>).MakeGenericType(type); // the Nullable<T>
                    // type is already the T
                }
                else
                {
                    secondary = type; // the Nullable<T>
                    type = underlying; // the T
                }
            }

            var snapshot = typeHandlers;
            if (snapshot.TryGetValue(type, out var oldValue) && converter == oldValue) return; // nothing to do

            var newCopy = clone ? new Dictionary<Type, IDbTypeConverter>(snapshot) : snapshot;

            typeof(TypeHandlerCache<>).MakeGenericType(type).GetMethod(nameof(TypeHandlerCache<int>.SetHandler), BindingFlags.Static | BindingFlags.NonPublic)
                                      .Invoke(null, new object[] { converter });
            if (secondary != null)
            {
                typeof(TypeHandlerCache<>).MakeGenericType(secondary)
                                          .GetMethod(nameof(TypeHandlerCache<int>.SetHandler), BindingFlags.Static | BindingFlags.NonPublic)
                                          .Invoke(null, new object[] { converter });
            }

            if (converter == null)
            {
                newCopy.Remove(type);
                if (secondary != null) newCopy.Remove(secondary);
            }
            else
            {
                newCopy[type] = converter;
                if (secondary != null) newCopy[secondary] = converter;
            }

            typeHandlers = newCopy;
        }

        /// <summary>
        /// Configure the specified type to be processed by a custom handler.
        /// </summary>
        /// <typeparam name="T">The type to handle.</typeparam>
        /// <param name="converter">The handler for the type <typeparamref name="T"/>.</param>
        public static void AddTypeHandler<T>(DbTypeConverter<T> converter) => AddTypeHandlerImpl(typeof(T), converter, true);

        /// <summary>
        /// Get the DbType that maps to a given value.
        /// </summary>
        /// <param name="value">The object to get a corresponding database type for.</param>
        public static DbType GetDbType(object value)
        {
            if (value == null || value is DBNull)
            {
                return DbType.Object;
            }

            return LookupDbType(value.GetType(), "n/a", false, out var handler);
        }

        /// <summary>
        /// OBSOLETE: For internal usage only. Lookup the DbType and handler for a given Type and member
        /// </summary>
        /// <param name="type">The type to lookup.</param>
        /// <param name="name">The name (for error messages).</param>
        /// <param name="demand">Whether to demand a value (throw if missing).</param>
        /// <param name="converter">The handler for <paramref name="type"/>.</param>
        public static DbType LookupDbType(Type type, string name, bool demand, out IDbTypeConverter converter)
        {
            converter = null;
            var nullUnderlyingType = Nullable.GetUnderlyingType(type);
            if (nullUnderlyingType != null) type = nullUnderlyingType;
            if (type.IsEnum() && !typeMap.ContainsKey(type))
            {
                type = Enum.GetUnderlyingType(type);
            }

            if (typeMap.TryGetValue(type, out var dbType))
            {
                return dbType;
            }

            if (type.FullName == LinqBinary)
            {
                return DbType.Binary;
            }

            if (typeHandlers.TryGetValue(type, out converter))
            {
                return DbType.Object;
            }

            if (typeof(IEnumerable).IsAssignableFrom(type))
            {
                return DynamicParameters.EnumerableMultiParameter;
            }

            if (demand)
            {
                throw new NotSupportedException($"The member {name} of type {type.FullName} cannot be used as a parameter value");
            }

            return DbType.Object;
        }

        public static bool ContainsTypeMap(Type type)
        {
            return typeMap.ContainsKey(type);
        }

        public static bool ContainsHandler(Type type)
        {
            return typeHandlers.ContainsKey(type);
        }

        public static bool TryGetHandler(Type type, out IDbTypeConverter converter)
        {
            return typeHandlers.TryGetValue(type, out converter);
        }
    }
}