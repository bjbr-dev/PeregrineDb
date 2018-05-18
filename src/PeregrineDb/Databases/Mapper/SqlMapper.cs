namespace PeregrineDb.Databases.Mapper
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Data;
    using PeregrineDb.Mapping;

    /// <remarks>
    /// Originally copied from Dapper.Net (https://github.com/StackExchange/dapper-dot-net) under the apache 2 license (http://www.apache.org/licenses/LICENSE-2.0).
    /// The code has been significantly altered.
    /// </remarks>
    internal static class SqlMapper
    {
        internal static int GetColumnHash(IDataReader reader, int startBound = 0, int length = -1)
        {
            unchecked
            {
                var max = length < 0 ? reader.FieldCount : startBound + length;
                var hash = (-37 * startBound) + max;
                for (var i = startBound; i < max; i++)
                {
                    object tmp = reader.GetName(i);
                    hash = (-79 * ((hash * 31) + (tmp?.GetHashCode() ?? 0))) + (reader.GetFieldType(i)?.GetHashCode() ?? 0);
                }

                return hash;
            }
        }

        public static CacheInfo GetCacheInfo(Identity identity, object exampleParameters, bool addToCache)
        {
            if (!QueryCache.TryGetQueryCache(identity, out var info))
            {
                if (IsEnumerable(exampleParameters))
                {
                    throw new InvalidOperationException("An enumerable sequence of parameters (arrays, lists, etc) is not allowed in this context");
                }

                info = new CacheInfo();
                if (identity.parametersType != null)
                {
                    Action<IDbCommand, object> reader;
                    if (exampleParameters is IDynamicParameters)
                    {
                        reader = (cmd, obj) => ((IDynamicParameters)obj).AddParameters(cmd, identity);
                    }
                    else if (exampleParameters is IEnumerable<KeyValuePair<string, object>>)
                    {
                        reader = (cmd, obj) =>
                        {
                            IDynamicParameters mapped = new Mapping.DynamicParameters(obj);
                            mapped.AddParameters(cmd, identity);
                        };
                    }
                    else
                    {
                        reader = TypeMapper.CreateParamInfoGenerator(identity, false, true);
                    }

                    info.ParamReader = reader;
                }

                if (addToCache)
                {
                    QueryCache.SetQueryCache(identity, info);
                }
            }

            return info;

            bool IsEnumerable(object param)
            {
                return param is IEnumerable
                       && !(param is string
                            || param is IEnumerable<KeyValuePair<string, object>>
                            || param is IDynamicParameters);
            }
        }

        internal static IEqualityComparer<string> connectionStringComparer = StringComparer.Ordinal;
    }
}