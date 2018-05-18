namespace PeregrineDb.Databases.Mapper
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Data;
    using System.Globalization;
    using System.Linq;
    using PeregrineDb.Mapping;

#if NETSTANDARD1_3
    using DataException = System.InvalidOperationException;
#endif

    /// <remarks>
    /// Originally copied from Dapper.Net (https://github.com/StackExchange/dapper-dot-net) under the apache 2 license (http://www.apache.org/licenses/LICENSE-2.0)
    /// </remarks>
    internal static partial class SqlMapper
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

        public static IEnumerable GetMultiExec(object param)
        {
            return param is IEnumerable enumerable
                   && !(enumerable is string
                        || enumerable is IEnumerable<KeyValuePair<string, object>>
                        || enumerable is IDynamicParameters)
                ? enumerable
                : null;
        }

        public static T ExecuteScalarImpl<T>(IDbConnection cnn, ref CommandDefinition command)
        {
            Action<IDbCommand, object> paramReader = null;
            var param = command.Parameters;
            if (param != null)
            {
                var identity = new Identity(command.CommandText, command.CommandType, cnn, null, param.GetType(), null);
                paramReader = GetCacheInfo(identity, command.Parameters, true).ParamReader;
            }

            object result;
            using (var cmd = command.SetupCommand(cnn, paramReader))
            {
                result = cmd.ExecuteScalar();
            }

            return TypeMapper.Parse<T>(result);
        }

        public static IEnumerable<T> QueryImpl<T>(this IDbConnection cnn, CommandDefinition command, Type effectiveType)
        {
            var param = command.Parameters;
            var identity = new Identity(command.CommandText, command.CommandType, cnn, effectiveType, param?.GetType(), null);
            var info = GetCacheInfo(identity, param, true);

            IDbCommand cmd = null;
            IDataReader reader = null;

            try
            {
                cmd = command.SetupCommand(cnn, info.ParamReader);

                reader = cmd.ExecuteReader(MapperSettings.Instance.GetBehavior(CommandBehavior.SequentialAccess | CommandBehavior.SingleResult));
                var tuple = info.Deserializer;
                var hash = GetColumnHash(reader);
                if (tuple.Func == null || tuple.Hash != hash)
                {
                    if (reader.FieldCount == 0) //https://code.google.com/p/dapper-dot-net/issues/detail?id=57
                    {
                        yield break;
                    }

                    tuple = info.Deserializer = new DeserializerState(hash, TypeMapper.GetDeserializer(effectiveType, reader, 0, -1, false));
                    if (true)
                    {
                        QueryCache.SetQueryCache(identity, info);
                    }
                }

                var func = tuple.Func;
                var convertToType = Nullable.GetUnderlyingType(effectiveType) ?? effectiveType;
                while (reader.Read())
                {
                    var val = func(reader);
                    if (val == null || val is T)
                    {
                        yield return (T)val;
                    }
                    else
                    {
                        yield return (T)Convert.ChangeType(val, convertToType, CultureInfo.InvariantCulture);
                    }
                }

                while (reader.NextResult())
                {
                    /* ignore subsequent result sets */
                }

                // happy path; close the reader cleanly - no
                // need for "Cancel" etc
                reader.Dispose();
                reader = null;
            }
            finally
            {
                if (reader != null)
                {
                    if (!reader.IsClosed)
                    {
                        try
                        {
                            cmd.Cancel();
                        }
                        catch
                        {
                            /* don't spoil the existing exception */
                        }
                    }

                    reader.Dispose();
                }

                cmd?.Dispose();
            }
        }

        [Flags]
        internal enum Row
        {
            First = 0,
            FirstOrDefault = 1, //  & FirstOrDefault != 0: allow zero rows
            Single = 2, // & Single != 0: demand at least one row
            SingleOrDefault = 3
        }

        private static readonly int[] ErrTwoRows = new int[2], ErrZeroRows = new int[0];

        private static void ThrowMultipleRows(Row row)
        {
            switch (row)
            {
                // get the standard exception from the runtime
                case Row.Single:
                    ErrTwoRows.Single();
                    break;
                case Row.SingleOrDefault:
                    ErrTwoRows.SingleOrDefault();
                    break;
                default:
                    throw new InvalidOperationException();
            }
        }

        private static void ThrowZeroRows(Row row)
        {
            switch (row)
            {
                // get the standard exception from the runtime
                case Row.First:
                    ErrZeroRows.First();
                    break;
                case Row.Single:
                    ErrZeroRows.Single();
                    break;
                default:
                    throw new InvalidOperationException();
            }
        }

        public static T QueryRowImpl<T>(IDbConnection cnn, Row row, ref CommandDefinition command, Type effectiveType)
        {
            var param = command.Parameters;
            var identity = new Identity(command.CommandText, command.CommandType, cnn, effectiveType, param?.GetType(), null);
            var info = GetCacheInfo(identity, param, true);

            IDbCommand cmd = null;
            IDataReader reader = null;

            try
            {
                cmd = command.SetupCommand(cnn, info.ParamReader);

                reader = cmd.ExecuteReader(MapperSettings.Instance.GetBehavior((row & Row.Single) != 0
                    ? CommandBehavior.SequentialAccess | CommandBehavior.SingleResult // need to allow multiple rows, to check fail condition
                    : CommandBehavior.SequentialAccess | CommandBehavior.SingleResult | CommandBehavior.SingleRow));

                T result = default;
                if (reader.Read() && reader.FieldCount != 0)
                {
                    var tuple = info.Deserializer;
                    var hash = GetColumnHash(reader);
                    if (tuple.Func == null || tuple.Hash != hash)
                    {
                        tuple = info.Deserializer = new DeserializerState(hash, TypeMapper.GetDeserializer(effectiveType, reader, 0, -1, false));
                        if (true) QueryCache.SetQueryCache(identity, info);
                    }

                    var func = tuple.Func;
                    var val = func(reader);
                    if (val == null || val is T)
                    {
                        result = (T)val;
                    }
                    else
                    {
                        var convertToType = Nullable.GetUnderlyingType(effectiveType) ?? effectiveType;
                        result = (T)Convert.ChangeType(val, convertToType, CultureInfo.InvariantCulture);
                    }

                    if ((row & Row.Single) != 0 && reader.Read()) ThrowMultipleRows(row);
                    while (reader.Read())
                    {
                        /* ignore subsequent rows */
                    }
                }
                else if ((row & Row.FirstOrDefault) == 0) // demanding a row, and don't have one
                {
                    ThrowZeroRows(row);
                }

                while (reader.NextResult())
                {
                    /* ignore subsequent result sets */
                }

                // happy path; close the reader cleanly - no
                // need for "Cancel" etc
                reader.Dispose();
                reader = null;

                return result;
            }
            finally
            {
                if (reader != null)
                {
                    if (!reader.IsClosed)
                    {
                        try
                        {
                            cmd.Cancel();
                        }
                        catch
                        {
                            /* don't spoil the existing exception */
                        }
                    }

                    reader.Dispose();
                }

                cmd?.Dispose();
            }
        }

        public static CacheInfo GetCacheInfo(Identity identity, object exampleParameters, bool addToCache)
        {
            if (!QueryCache.TryGetQueryCache(identity, out var info))
            {
                if (GetMultiExec(exampleParameters) != null)
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
        }

        internal static IEqualityComparer<string> connectionStringComparer = StringComparer.Ordinal;
    }
}