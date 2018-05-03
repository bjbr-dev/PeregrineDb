namespace PeregrineDb.Databases.Mapper
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Data;
    using System.Globalization;
    using System.Linq;
    using System.Reflection;
    using System.Reflection.Emit;
    using System.Text;
    using System.Text.RegularExpressions;
    using PeregrineDb.Mapping;

#if NETSTANDARD1_3
    using DataException = System.InvalidOperationException;
#endif

    /// <remarks>
    /// Originally copied from Dapper.Net (https://github.com/StackExchange/dapper-dot-net) under the apache 2 license (http://www.apache.org/licenses/LICENSE-2.0)
    /// </remarks>
    internal static partial class SqlMapper
    {
        private class PropertyInfoByNameComparer
            : IComparer<PropertyInfo>
        {
            public int Compare(PropertyInfo x, PropertyInfo y) => string.CompareOrdinal(x.Name, y.Name);
        }

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

        private static IEnumerable GetMultiExec(object param)
        {
            return param is IEnumerable enumerable
                   && !(enumerable is string
                        || enumerable is IEnumerable<KeyValuePair<string, object>>
                        || enumerable is IDynamicParameters)
                ? enumerable
                : null;
        }

        public static int ExecuteImpl(this IDbConnection cnn, ref CommandDefinition command)
        {
            var param = command.Parameters;
            var multiExec = GetMultiExec(param);
            Identity identity;
            CacheInfo info = null;
            if (multiExec != null)
            {
                var isFirst = true;
                var total = 0;

                using (var cmd = command.SetupCommand(cnn, null))
                {
                    string masterSql = null;
                    foreach (var obj in multiExec)
                    {
                        if (isFirst)
                        {
                            masterSql = cmd.CommandText;
                            isFirst = false;
                            identity = new Identity(command.CommandText, cmd.CommandType, cnn, null, obj.GetType(), null);
                            info = GetCacheInfo(identity, obj, true);
                        }
                        else
                        {
                            cmd.CommandText = masterSql; // because we do magic replaces on "in" etc
                            cmd.Parameters.Clear(); // current code is Add-tastic
                        }

                        info.ParamReader(cmd, obj);
                        total += cmd.ExecuteNonQuery();
                    }
                }

                return total;
            }

            // nice and simple
            if (param != null)
            {
                identity = new Identity(command.CommandText, command.CommandType, cnn, null, param.GetType(), null);
                info = GetCacheInfo(identity, param, true);
            }

            using (var cmd = command.SetupCommand(cnn, param == null ? null : info.ParamReader))
            {
                return cmd.ExecuteNonQuery();
            }
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

                    tuple = info.Deserializer = new DeserializerState(hash, GetDeserializer(effectiveType, reader, 0, -1, false));
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
                        tuple = info.Deserializer = new DeserializerState(hash, GetDeserializer(effectiveType, reader, 0, -1, false));
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

        private static CacheInfo GetCacheInfo(Identity identity, object exampleParameters, bool addToCache)
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
                            IDynamicParameters mapped = new DynamicParameters(obj);
                            mapped.AddParameters(cmd, identity);
                        };
                    }
                    else
                    {
                        reader = CreateParamInfoGenerator(identity, false, true);
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

        private static Func<IDataReader, object> GetDeserializer(Type type, IDataReader reader, int startBound, int length, bool returnNullIfFirstMissing)
        {
            Type underlyingType = null;
            if (!(TypeProvider.ContainsTypeMap(type) || type.IsEnum() || type.FullName == TypeProvider.LinqBinary
                  || (type.IsValueType() && (underlyingType = Nullable.GetUnderlyingType(type)) != null && underlyingType.IsEnum())))
            {
                if (TypeProvider.TryGetHandler(type, out var handler))
                {
                    return GetHandlerDeserializer(handler, type, startBound);
                }

                return GetTypeDeserializer(type, reader, startBound, length, returnNullIfFirstMissing);
            }

            return GetStructDeserializer(type, underlyingType ?? type, startBound);
        }

        private static Func<IDataReader, object> GetHandlerDeserializer(IDbTypeConverter converter, Type type, int startBound)
        {
            return reader => converter.Parse(type, reader.GetValue(startBound));
        }

        private static Exception MultiMapException(IDataRecord reader)
        {
            var hasFields = false;
            try
            {
                hasFields = reader != null && reader.FieldCount != 0;
            }
            catch
            {
                /* don't throw when trying to throw */
            }

            if (hasFields)
            {
                return new ArgumentException("When using the multi-mapping APIs ensure you set the splitOn param if you have keys other than Id", "splitOn");
            }

            return new InvalidOperationException("No columns were selected");
        }

        /// <summary>
        /// Internal use only.
        /// </summary>
        /// <param name="value">The object to convert to a character.</param>
        public static char ReadChar(object value)
        {
            if (value == null || value is DBNull)
            {
                throw new ArgumentNullException(nameof(value));
            }

            if (!(value is string s) || s.Length != 1)
            {
                throw new ArgumentException("A single-character was expected", nameof(value));
            }

            return s[0];
        }

        /// <summary>
        /// Internal use only.
        /// </summary>
        /// <param name="value">The object to convert to a character.</param>
        public static char? ReadNullableChar(object value)
        {
            if (value == null || value is DBNull)
            {
                return null;
            }

            var s = value as string;
            if (s == null || s.Length != 1)
            {
                throw new ArgumentException("A single-character was expected", nameof(value));
            }

            return s[0];
        }

        /// <summary>
        /// Internal use only.
        /// </summary>
        /// <param name="parameters">The parameter collection to search in.</param>
        /// <param name="command">The command for this fetch.</param>
        /// <param name="name">The name of the parameter to get.</param>
        public static IDbDataParameter FindOrAddParameter(IDataParameterCollection parameters, IDbCommand command, string name)
        {
            IDbDataParameter result;
            if (parameters.Contains(name))
            {
                result = (IDbDataParameter)parameters[name];
            }
            else
            {
                result = command.CreateParameter();
                result.ParameterName = name;
                parameters.Add(result);
            }

            return result;
        }

        /// <summary>
        /// Internal use only.
        /// </summary>
        /// <param name="command">The command to pack parameters for.</param>
        /// <param name="namePrefix">The name prefix for these parameters.</param>
        /// <param name="value">The parameter value can be an <see cref="IEnumerable{T}"/></param>
        public static void PackListParameters(IDbCommand command, string namePrefix, object value)
        {
            var arrayParm = command.CreateParameter();
            arrayParm.Value = SanitizeParameterValue(value);
            arrayParm.ParameterName = namePrefix;
            command.Parameters.Add(arrayParm);
        }

        /// <summary>
        /// OBSOLETE: For internal usage only. Sanitizes the paramter value with proper type casting.
        /// </summary>
        /// <param name="value">The value to sanitize.</param>
        public static object SanitizeParameterValue(object value)
        {
            if (value == null)
            {
                return DBNull.Value;
            }

            if (value is Enum)
            {
                var typeCode = ((IConvertible)value).GetTypeCode();
                switch (typeCode)
                {
                    case TypeCode.Byte:
                        return (byte)value;
                    case TypeCode.SByte:
                        return (sbyte)value;
                    case TypeCode.Int16:
                        return (short)value;
                    case TypeCode.Int32:
                        return (int)value;
                    case TypeCode.Int64:
                        return (long)value;
                    case TypeCode.UInt16:
                        return (ushort)value;
                    case TypeCode.UInt32:
                        return (uint)value;
                    case TypeCode.UInt64:
                        return (ulong)value;
                }
            }

            return value;
        }

        private static IEnumerable<PropertyInfo> FilterParameters(IEnumerable<PropertyInfo> parameters, string sql)
        {
            return parameters.Where(p => Regex.IsMatch(sql, @"[?@:]" + p.Name + @"([^\p{L}\p{N}_]+|$)",
                                 RegexOptions.IgnoreCase | RegexOptions.Multiline | RegexOptions.CultureInvariant))
                             .ToList();
        }

        /// <summary>
        /// Convert numeric values to their string form for SQL literal purposes.
        /// </summary>
        /// <param name="value">The value to get a string for.</param>
        public static string Format(object value)
        {
            if (value == null)
            {
                return "null";
            }

            switch (TypeExtensions.GetTypeCode(value.GetType()))
            {
#if !NETSTANDARD1_3
                case TypeCode.DBNull:
                    return "null";
#endif
                case TypeCode.Boolean:
                    return ((bool)value) ? "1" : "0";
                case TypeCode.Byte:
                    return ((byte)value).ToString(CultureInfo.InvariantCulture);
                case TypeCode.SByte:
                    return ((sbyte)value).ToString(CultureInfo.InvariantCulture);
                case TypeCode.UInt16:
                    return ((ushort)value).ToString(CultureInfo.InvariantCulture);
                case TypeCode.Int16:
                    return ((short)value).ToString(CultureInfo.InvariantCulture);
                case TypeCode.UInt32:
                    return ((uint)value).ToString(CultureInfo.InvariantCulture);
                case TypeCode.Int32:
                    return ((int)value).ToString(CultureInfo.InvariantCulture);
                case TypeCode.UInt64:
                    return ((ulong)value).ToString(CultureInfo.InvariantCulture);
                case TypeCode.Int64:
                    return ((long)value).ToString(CultureInfo.InvariantCulture);
                case TypeCode.Single:
                    return ((float)value).ToString(CultureInfo.InvariantCulture);
                case TypeCode.Double:
                    return ((double)value).ToString(CultureInfo.InvariantCulture);
                case TypeCode.Decimal:
                    return ((decimal)value).ToString(CultureInfo.InvariantCulture);
                default:
                    var multiExec = GetMultiExec(value);
                    if (multiExec != null)
                    {
                        StringBuilder sb = null;
                        var first = true;
                        foreach (var subval in multiExec)
                        {
                            if (first)
                            {
                                sb = StringBuilderPool.Acquire().Append('(');
                                first = false;
                            }
                            else
                            {
                                sb.Append(',');
                            }

                            sb.Append(Format(subval));
                        }

                        if (first)
                        {
                            return "(select null where 1=0)";
                        }
                        else
                        {
                            return sb.Append(')').__ToStringRecycle();
                        }
                    }

                    throw new NotSupportedException(value.GetType().Name);
            }
        }

        private static bool IsValueTuple(Type type) => type?.IsValueType() == true && type.FullName.StartsWith("System.ValueTuple`", StringComparison.Ordinal);

        private static List<IMemberMap> GetValueTupleMembers(Type type, string[] names)
        {
            var fields = type.GetFields(BindingFlags.Public | BindingFlags.Instance);
            var result = new List<IMemberMap>(names.Length);
            for (var i = 0; i < names.Length; i++)
            {
                FieldInfo field = null;
                var name = "Item" + (i + 1).ToString(CultureInfo.InvariantCulture);
                foreach (var test in fields)
                {
                    if (test.Name == name)
                    {
                        field = test;
                        break;
                    }
                }

                result.Add(field == null ? null : new SimpleMemberMap(string.IsNullOrWhiteSpace(names[i]) ? name : names[i], field));
            }

            return result;
        }

        internal static Action<IDbCommand, object> CreateParamInfoGenerator(
            Identity identity,
            bool checkForDuplicates,
            bool removeUnused)
        {
            var type = identity.parametersType;

            if (IsValueTuple(type))
            {
                throw new NotSupportedException(
                    "ValueTuple should not be used for parameters - the language-level names are not available to use as parameter names, and it adds unnecessary boxing");
            }

            var filterParams = removeUnused && identity.commandType.GetValueOrDefault(CommandType.Text) == CommandType.Text;
            var dm = new DynamicMethod("ParamInfo" + Guid.NewGuid(), null, new[] { typeof(IDbCommand), typeof(object) }, type, true);

            var il = dm.GetILGenerator();

            var isStruct = type.IsValueType();
            var haveInt32Arg1 = false;
            il.Emit(OpCodes.Ldarg_1); // stack is now [untyped-param]
            if (isStruct)
            {
                il.DeclareLocal(type.MakePointerType());
                il.Emit(OpCodes.Unbox, type); // stack is now [typed-param]
            }
            else
            {
                il.DeclareLocal(type); // 0
                il.Emit(OpCodes.Castclass, type); // stack is now [typed-param]
            }

            il.Emit(OpCodes.Stloc_0); // stack is now empty

            il.Emit(OpCodes.Ldarg_0); // stack is now [command]
            il.EmitCall(OpCodes.Callvirt, typeof(IDbCommand).GetProperty(nameof(IDbCommand.Parameters)).GetGetMethod(), null); // stack is now [parameters]

            var allTypeProps = type.GetProperties();
            var propsList = new List<PropertyInfo>(allTypeProps.Length);
            for (var i = 0; i < allTypeProps.Length; ++i)
            {
                var p = allTypeProps[i];
                if (p.GetIndexParameters().Length == 0)
                    propsList.Add(p);
            }

            var ctors = type.GetConstructors();
            ParameterInfo[] ctorParams;
            IEnumerable<PropertyInfo> props = null;
            // try to detect tuple patterns, e.g. anon-types, and use that to choose the order
            // otherwise: alphabetical
            if (ctors.Length == 1 && propsList.Count == (ctorParams = ctors[0].GetParameters()).Length)
            {
                // check if reflection was kind enough to put everything in the right order for us
                var ok = true;
                for (var i = 0; i < propsList.Count; i++)
                {
                    if (!string.Equals(propsList[i].Name, ctorParams[i].Name, StringComparison.OrdinalIgnoreCase))
                    {
                        ok = false;
                        break;
                    }
                }

                if (ok)
                {
                    // pre-sorted; the reflection gods have smiled upon us
                    props = propsList;
                }
                else
                {
                    // might still all be accounted for; check the hard way
                    var positionByName = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
                    foreach (var param in ctorParams)
                    {
                        positionByName[param.Name] = param.Position;
                    }

                    if (positionByName.Count == propsList.Count)
                    {
                        var positions = new int[propsList.Count];
                        ok = true;
                        for (var i = 0; i < propsList.Count; i++)
                        {
                            if (!positionByName.TryGetValue(propsList[i].Name, out var pos))
                            {
                                ok = false;
                                break;
                            }

                            positions[i] = pos;
                        }

                        if (ok)
                        {
                            props = propsList.ToArray();
                            Array.Sort(positions, (PropertyInfo[])props);
                        }
                    }
                }
            }

            if (props == null)
            {
                propsList.Sort(new SqlMapper.PropertyInfoByNameComparer());
                props = propsList;
            }

            if (filterParams)
            {
                props = FilterParameters(props, identity.sql);
            }

            var callOpCode = isStruct ? OpCodes.Call : OpCodes.Callvirt;
            foreach (var prop in props)
            {
                if (typeof(ICustomQueryParameter).IsAssignableFrom(prop.PropertyType))
                {
                    il.Emit(OpCodes.Ldloc_0); // stack is now [parameters] [typed-param]
                    il.Emit(callOpCode, prop.GetGetMethod()); // stack is [parameters] [custom]
                    il.Emit(OpCodes.Ldarg_0); // stack is now [parameters] [custom] [command]
                    il.Emit(OpCodes.Ldstr, prop.Name); // stack is now [parameters] [custom] [command] [name]
                    il.EmitCall(OpCodes.Callvirt, prop.PropertyType.GetMethod(nameof(ICustomQueryParameter.AddParameter)), null); // stack is now [parameters]
                    continue;
                }

                var dbType = TypeProvider.LookupDbType(prop.PropertyType, prop.Name, true, out var handler);
                if (dbType == DynamicParameters.EnumerableMultiParameter)
                {
                    // this actually represents special handling for list types;
                    il.Emit(OpCodes.Ldarg_0); // stack is now [parameters] [command]
                    il.Emit(OpCodes.Ldstr, prop.Name); // stack is now [parameters] [command] [name]
                    il.Emit(OpCodes.Ldloc_0); // stack is now [parameters] [command] [name] [typed-param]
                    il.Emit(callOpCode, prop.GetGetMethod()); // stack is [parameters] [command] [name] [typed-value]
                    if (prop.PropertyType.IsValueType())
                    {
                        il.Emit(OpCodes.Box, prop.PropertyType); // stack is [parameters] [command] [name] [boxed-value]
                    }

                    il.EmitCall(OpCodes.Call, typeof(SqlMapper).GetMethod(nameof(SqlMapper.PackListParameters)), null); // stack is [parameters]
                    continue;
                }

                il.Emit(OpCodes.Dup); // stack is now [parameters] [parameters]

                il.Emit(OpCodes.Ldarg_0); // stack is now [parameters] [parameters] [command]

                if (checkForDuplicates)
                {
                    // need to be a little careful about adding; use a utility method
                    il.Emit(OpCodes.Ldstr, prop.Name); // stack is now [parameters] [parameters] [command] [name]
                    il.EmitCall(OpCodes.Call, typeof(SqlMapper).GetMethod(nameof(SqlMapper.FindOrAddParameter)), null); // stack is [parameters] [parameter]
                }
                else
                {
                    // no risk of duplicates; just blindly add
                    il.EmitCall(OpCodes.Callvirt, typeof(IDbCommand).GetMethod(nameof(IDbCommand.CreateParameter)), null); // stack is now [parameters] [parameters] [parameter]

                    il.Emit(OpCodes.Dup); // stack is now [parameters] [parameters] [parameter] [parameter]
                    il.Emit(OpCodes.Ldstr, prop.Name); // stack is now [parameters] [parameters] [parameter] [parameter] [name]
                    il.EmitCall(OpCodes.Callvirt, typeof(IDataParameter).GetProperty(nameof(IDataParameter.ParameterName)).GetSetMethod(), null); // stack is now [parameters] [parameters] [parameter]
                }

                if (dbType != DbType.Time && handler == null) // https://connect.microsoft.com/VisualStudio/feedback/details/381934/sqlparameter-dbtype-dbtype-time-sets-the-parameter-to-sqldbtype-datetime-instead-of-sqldbtype-time
                {
                    il.Emit(OpCodes.Dup); // stack is now [parameters] [[parameters]] [parameter] [parameter]
                    if (dbType == DbType.Object && prop.PropertyType == typeof(object)) // includes dynamic
                    {
                        // look it up from the param value
                        il.Emit(OpCodes.Ldloc_0); // stack is now [parameters] [[parameters]] [parameter] [parameter] [typed-param]
                        il.Emit(callOpCode, prop.GetGetMethod()); // stack is [parameters] [[parameters]] [parameter] [parameter] [object-value]
                        il.Emit(OpCodes.Call, typeof(TypeProvider).GetMethod(nameof(TypeProvider.GetDbType), BindingFlags.Static | BindingFlags.Public)); // stack is now [parameters] [[parameters]] [parameter] [parameter] [db-type]
                    }
                    else
                    {
                        // constant value; nice and simple
                        EmitInt32(il, (int)dbType); // stack is now [parameters] [[parameters]] [parameter] [parameter] [db-type]
                    }

                    il.EmitCall(OpCodes.Callvirt, typeof(IDataParameter).GetProperty(nameof(IDataParameter.DbType)).GetSetMethod(), null); // stack is now [parameters] [[parameters]] [parameter]
                }

                il.Emit(OpCodes.Dup); // stack is now [parameters] [[parameters]] [parameter] [parameter]
                EmitInt32(il, (int)ParameterDirection.Input); // stack is now [parameters] [[parameters]] [parameter] [parameter] [dir]
                il.EmitCall(OpCodes.Callvirt, typeof(IDataParameter).GetProperty(nameof(IDataParameter.Direction)).GetSetMethod(),
                    null); // stack is now [parameters] [[parameters]] [parameter]

                il.Emit(OpCodes.Dup); // stack is now [parameters] [[parameters]] [parameter] [parameter]
                il.Emit(OpCodes.Ldloc_0); // stack is now [parameters] [[parameters]] [parameter] [parameter] [typed-param]
                il.Emit(callOpCode, prop.GetGetMethod()); // stack is [parameters] [[parameters]] [parameter] [parameter] [typed-value]
                bool checkForNull;
                if (prop.PropertyType.IsValueType())
                {
                    var propType = prop.PropertyType;
                    var nullType = Nullable.GetUnderlyingType(propType);
                    var callSanitize = false;

                    if ((nullType ?? propType).IsEnum())
                    {
                        if (nullType != null)
                        {
                            // Nullable<SomeEnum>; we want to box as the underlying type; that's just *hard*; for
                            // simplicity, box as Nullable<SomeEnum> and call SanitizeParameterValue
                            callSanitize = checkForNull = true;
                        }
                        else
                        {
                            checkForNull = false;
                            // non-nullable enum; we can do that! just box to the wrong type! (no, really)
                            switch (TypeExtensions.GetTypeCode(Enum.GetUnderlyingType(propType)))
                            {
                                case TypeCode.Byte:
                                    propType = typeof(byte);
                                    break;
                                case TypeCode.SByte:
                                    propType = typeof(sbyte);
                                    break;
                                case TypeCode.Int16:
                                    propType = typeof(short);
                                    break;
                                case TypeCode.Int32:
                                    propType = typeof(int);
                                    break;
                                case TypeCode.Int64:
                                    propType = typeof(long);
                                    break;
                                case TypeCode.UInt16:
                                    propType = typeof(ushort);
                                    break;
                                case TypeCode.UInt32:
                                    propType = typeof(uint);
                                    break;
                                case TypeCode.UInt64:
                                    propType = typeof(ulong);
                                    break;
                            }
                        }
                    }
                    else
                    {
                        checkForNull = nullType != null;
                    }

                    il.Emit(OpCodes.Box, propType); // stack is [parameters] [[parameters]] [parameter] [parameter] [boxed-value]
                    if (callSanitize)
                    {
                        checkForNull = false; // handled by sanitize
                        il.EmitCall(OpCodes.Call, typeof(SqlMapper).GetMethod(nameof(SanitizeParameterValue)), null);
                        // stack is [parameters] [[parameters]] [parameter] [parameter] [boxed-value]
                    }
                }
                else
                {
                    checkForNull = true; // if not a value-type, need to check
                }

                if (checkForNull)
                {
                    if ((dbType == DbType.String || dbType == DbType.AnsiString) && !haveInt32Arg1)
                    {
                        il.DeclareLocal(typeof(int));
                        haveInt32Arg1 = true;
                    }

                    // relative stack: [boxed value]
                    il.Emit(OpCodes.Dup); // relative stack: [boxed value] [boxed value]
                    var notNull = il.DefineLabel();
                    var allDone = (dbType == DbType.String || dbType == DbType.AnsiString) ? il.DefineLabel() : (Label?)null;
                    il.Emit(OpCodes.Brtrue_S, notNull);
                    // relative stack [boxed value = null]
                    il.Emit(OpCodes.Pop); // relative stack empty
                    il.Emit(OpCodes.Ldsfld, typeof(DBNull).GetField(nameof(DBNull.Value))); // relative stack [DBNull]
                    if (dbType == DbType.String || dbType == DbType.AnsiString)
                    {
                        EmitInt32(il, 0);
                        il.Emit(OpCodes.Stloc_1);
                    }

                    if (allDone != null) il.Emit(OpCodes.Br_S, allDone.Value);
                    il.MarkLabel(notNull);
                    if (prop.PropertyType == typeof(string))
                    {
                        il.Emit(OpCodes.Dup); // [string] [string]
                        il.EmitCall(OpCodes.Callvirt, typeof(string).GetProperty(nameof(string.Length)).GetGetMethod(), null); // [string] [length]
                        EmitInt32(il, DbString.DefaultLength); // [string] [length] [4000]
                        il.Emit(OpCodes.Cgt); // [string] [0 or 1]
                        Label isLong = il.DefineLabel(), lenDone = il.DefineLabel();
                        il.Emit(OpCodes.Brtrue_S, isLong);
                        EmitInt32(il, DbString.DefaultLength); // [string] [4000]
                        il.Emit(OpCodes.Br_S, lenDone);
                        il.MarkLabel(isLong);
                        EmitInt32(il, -1); // [string] [-1]
                        il.MarkLabel(lenDone);
                        il.Emit(OpCodes.Stloc_1); // [string]
                    }

                    if (prop.PropertyType.FullName == TypeProvider.LinqBinary)
                    {
                        il.EmitCall(OpCodes.Callvirt, prop.PropertyType.GetMethod("ToArray", BindingFlags.Public | BindingFlags.Instance), null);
                    }

                    if (allDone != null) il.MarkLabel(allDone.Value);
                    // relative stack [boxed value or DBNull]
                }

                if (handler != null)
                {
                    il.Emit(OpCodes.Call, typeof(TypeHandlerCache<>).MakeGenericType(prop.PropertyType).GetMethod(nameof(TypeHandlerCache<int>.SetValue))); // stack is now [parameters] [[parameters]] [parameter]
                }
                else
                {
                    il.EmitCall(OpCodes.Callvirt, typeof(IDataParameter).GetProperty(nameof(IDataParameter.Value)).GetSetMethod(), null); // stack is now [parameters] [[parameters]] [parameter]
                }

                if (prop.PropertyType == typeof(string))
                {
                    var endOfSize = il.DefineLabel();
                    // don't set if 0
                    il.Emit(OpCodes.Ldloc_1); // [parameters] [[parameters]] [parameter] [size]
                    il.Emit(OpCodes.Brfalse_S, endOfSize); // [parameters] [[parameters]] [parameter]

                    il.Emit(OpCodes.Dup); // stack is now [parameters] [[parameters]] [parameter] [parameter]
                    il.Emit(OpCodes.Ldloc_1); // stack is now [parameters] [[parameters]] [parameter] [parameter] [size]
                    il.EmitCall(OpCodes.Callvirt, typeof(IDbDataParameter).GetProperty(nameof(IDbDataParameter.Size)).GetSetMethod(),
                        null); // stack is now [parameters] [[parameters]] [parameter]

                    il.MarkLabel(endOfSize);
                }

                if (checkForDuplicates)
                {
                    // stack is now [parameters] [parameter]
                    il.Emit(OpCodes.Pop); // don't need parameter any more
                }
                else
                {
                    // stack is now [parameters] [parameters] [parameter]
                    // blindly add
                    il.EmitCall(OpCodes.Callvirt, typeof(IList).GetMethod(nameof(IList.Add)), null); // stack is now [parameters]
                    il.Emit(OpCodes.Pop); // IList.Add returns the new index (int); we don't care
                }
            }

            // stack is currently [parameters]
            il.Emit(OpCodes.Pop); // stack is now empty

            il.Emit(OpCodes.Ret);
            return (Action<IDbCommand, object>)dm.CreateDelegate(typeof(Action<IDbCommand, object>));
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

            return Parse<T>(result);
        }

        private static Func<IDataReader, object> GetStructDeserializer(Type type, Type effectiveType, int index)
        {
            // no point using special per-type handling here; it boils down to the same, plus not all are supported anyway (see: SqlDataReader.GetChar - not supported!)
            if (type == typeof(char))
            {
                // this *does* need special handling, though
                return r => ReadChar(r.GetValue(index));
            }

            if (type == typeof(char?))
            {
                return r => ReadNullableChar(r.GetValue(index));
            }

            if (type.FullName == TypeProvider.LinqBinary)
            {
                return r => Activator.CreateInstance(type, r.GetValue(index));
            }

            if (effectiveType.IsEnum())
            {
                // assume the value is returned as the correct type (int/byte/etc), but box back to the typed enum
                return r =>
                {
                    var val = r.GetValue(index);
                    if (val is float || val is double || val is decimal)
                    {
                        val = Convert.ChangeType(val, Enum.GetUnderlyingType(effectiveType), CultureInfo.InvariantCulture);
                    }

                    return val is DBNull ? null : Enum.ToObject(effectiveType, val);
                };
            }

            if (TypeProvider.TryGetHandler(type, out var handler))
            {
                return r =>
                {
                    var val = r.GetValue(index);
                    return val is DBNull ? null : handler.Parse(type, val);
                };
            }

            return r =>
            {
                var val = r.GetValue(index);
                return val is DBNull ? null : val;
            };
        }

        private static T Parse<T>(object value)
        {
            if (value == null || value is DBNull) return default(T);
            if (value is T) return (T)value;
            var type = typeof(T);
            type = Nullable.GetUnderlyingType(type) ?? type;
            if (type.IsEnum())
            {
                if (value is float || value is double || value is decimal)
                {
                    value = Convert.ChangeType(value, Enum.GetUnderlyingType(type), CultureInfo.InvariantCulture);
                }

                return (T)Enum.ToObject(type, value);
            }

            if (TypeProvider.TryGetHandler(type, out var handler))
            {
                return (T)handler.Parse(type, value);
            }

            return (T)Convert.ChangeType(value, type, CultureInfo.InvariantCulture);
        }

        private static readonly MethodInfo
            enumParse = typeof(Enum).GetMethod(nameof(Enum.Parse), new Type[] { typeof(Type), typeof(string), typeof(bool) }),
            getItem = typeof(IDataRecord).GetProperties(BindingFlags.Instance | BindingFlags.Public)
                                         .Where(p => p.GetIndexParameters().Length > 0 && p.GetIndexParameters()[0].ParameterType == typeof(int))
                                         .Select(p => p.GetGetMethod()).First();

        /// <summary>
        /// Gets type-map for the given type
        /// </summary>
        /// <returns>Type map instance, default is to create new instance of DefaultTypeMap</returns>
        public static Func<Type, ITypeMap> TypeMapProvider = type => new DefaultTypeMap(type);

        /// <summary>
        /// Gets type-map for the given <see cref="Type"/>.
        /// </summary>
        /// <param name="type">The type to get a map for.</param>
        /// <returns>Type map implementation, DefaultTypeMap instance if no override present</returns>
        public static ITypeMap GetTypeMap(Type type)
        {
            if (type == null) throw new ArgumentNullException(nameof(type));
            var map = (ITypeMap)_typeMaps[type];
            if (map == null)
            {
                lock (_typeMaps)
                {
                    // double-checked; store this to avoid reflection next time we see this type
                    // since multiple queries commonly use the same domain-entity/DTO/view-model type
                    map = (ITypeMap)_typeMaps[type];

                    if (map == null)
                    {
                        map = TypeMapProvider(type);
                        _typeMaps[type] = map;
                    }
                }
            }

            return map;
        }

        // use Hashtable to get free lockless reading
        private static readonly Hashtable _typeMaps = new Hashtable();

        /// <summary>
        /// Set custom mapping for type deserializers
        /// </summary>
        /// <param name="type">Entity type to override</param>
        /// <param name="map">Mapping rules impementation, null to remove custom map</param>
        public static void SetTypeMap(Type type, ITypeMap map)
        {
            if (type == null)
                throw new ArgumentNullException(nameof(type));

            if (map == null || map is DefaultTypeMap)
            {
                lock (_typeMaps)
                {
                    _typeMaps.Remove(type);
                }
            }
            else
            {
                lock (_typeMaps)
                {
                    _typeMaps[type] = map;
                }
            }

            QueryCache.PurgeQueryCacheByType(type);
        }

        /// <summary>
        /// Internal use only
        /// </summary>
        public static Func<IDataReader, object> GetTypeDeserializer(
            Type type,
            IDataReader reader,
            int startBound = 0,
            int length = -1,
            bool returnNullIfFirstMissing = false)
        {
            return TypeDeserializerCache.GetReader(type, reader, startBound, length, returnNullIfFirstMissing);
        }

        private static LocalBuilder GetTempLocal(ILGenerator il, ref Dictionary<Type, LocalBuilder> locals, Type type, bool initAndLoad)
        {
            if (type == null) throw new ArgumentNullException(nameof(type));
            locals = locals ?? new Dictionary<Type, LocalBuilder>();
            if (!locals.TryGetValue(type, out var found))
            {
                found = il.DeclareLocal(type);
                locals.Add(type, found);
            }

            if (initAndLoad)
            {
                il.Emit(OpCodes.Ldloca, (short)found.LocalIndex);
                il.Emit(OpCodes.Initobj, type);
                il.Emit(OpCodes.Ldloca, (short)found.LocalIndex);
                il.Emit(OpCodes.Ldobj, type);
            }

            return found;
        }

        internal static Func<IDataReader, object> GetTypeDeserializerImpl(
            Type type,
            IDataReader reader,
            int startBound = 0,
            int length = -1,
            bool returnNullIfFirstMissing = false)
        {
            var returnType = type.IsValueType() ? typeof(object) : type;
            var dm = new DynamicMethod("Deserialize" + Guid.NewGuid(), returnType, new[] { typeof(IDataReader) }, type, true);
            var il = dm.GetILGenerator();
            il.DeclareLocal(typeof(int));
            il.DeclareLocal(type);
            il.Emit(OpCodes.Ldc_I4_0);
            il.Emit(OpCodes.Stloc_0);

            if (length == -1)
            {
                length = reader.FieldCount - startBound;
            }

            if (reader.FieldCount <= startBound)
            {
                throw MultiMapException(reader);
            }

            var names = Enumerable.Range(startBound, length).Select(i => reader.GetName(i)).ToArray();

            var typeMap = GetTypeMap(type);

            var index = startBound;
            ConstructorInfo specializedConstructor = null;

            Dictionary<Type, LocalBuilder> structLocals = null;
            if (type.IsValueType())
            {
                il.Emit(OpCodes.Ldloca_S, (byte)1);
                il.Emit(OpCodes.Initobj, type);
            }
            else
            {
                var types = new Type[length];
                for (var i = startBound; i < startBound + length; i++)
                {
                    types[i - startBound] = reader.GetFieldType(i);
                }

                var explicitConstr = typeMap.FindExplicitConstructor();
                if (explicitConstr != null)
                {
                    var consPs = explicitConstr.GetParameters();
                    foreach (var p in consPs)
                    {
                        if (!p.ParameterType.IsValueType())
                        {
                            il.Emit(OpCodes.Ldnull);
                        }
                        else
                        {
                            GetTempLocal(il, ref structLocals, p.ParameterType, true);
                        }
                    }

                    il.Emit(OpCodes.Newobj, explicitConstr);
                    il.Emit(OpCodes.Stloc_1);
                }
                else
                {
                    var ctor = typeMap.FindConstructor(names, types);
                    if (ctor == null)
                    {
                        var proposedTypes = "(" + string.Join(", ", types.Select((t, i) => t.FullName + " " + names[i]).ToArray()) + ")";
                        throw new InvalidOperationException(
                            $"A parameterless default constructor or one matching signature {proposedTypes} is required for {type.FullName} materialization");
                    }

                    if (ctor.GetParameters().Length == 0)
                    {
                        il.Emit(OpCodes.Newobj, ctor);
                        il.Emit(OpCodes.Stloc_1);
                    }
                    else
                    {
                        specializedConstructor = ctor;
                    }
                }
            }

            il.BeginExceptionBlock();
            if (type.IsValueType())
            {
                il.Emit(OpCodes.Ldloca_S, (byte)1); // [target]
            }
            else if (specializedConstructor == null)
            {
                il.Emit(OpCodes.Ldloc_1); // [target]
            }

            var members = IsValueTuple(type)
                ? GetValueTupleMembers(type, names)
                : (specializedConstructor != null
                    ? names.Select(n => typeMap.GetConstructorParameter(specializedConstructor, n))
                    : names.Select(n => typeMap.GetMember(n))).ToList();

            // stack is now [target]

            var first = true;
            var allDone = il.DefineLabel();
            int enumDeclareLocal = -1, valueCopyLocal = il.DeclareLocal(typeof(object)).LocalIndex;
            foreach (var item in members)
            {
                if (item != null)
                {
                    if (specializedConstructor == null)
                        il.Emit(OpCodes.Dup); // stack is now [target][target]
                    var isDbNullLabel = il.DefineLabel();
                    var finishLabel = il.DefineLabel();

                    il.Emit(OpCodes.Ldarg_0); // stack is now [target][target][reader]
                    EmitInt32(il, index); // stack is now [target][target][reader][index]
                    il.Emit(OpCodes.Dup); // stack is now [target][target][reader][index][index]
                    il.Emit(OpCodes.Stloc_0); // stack is now [target][target][reader][index]
                    il.Emit(OpCodes.Callvirt, getItem); // stack is now [target][target][value-as-object]
                    il.Emit(OpCodes.Dup); // stack is now [target][target][value-as-object][value-as-object]
                    StoreLocal(il, valueCopyLocal);
                    var colType = reader.GetFieldType(index);
                    var memberType = item.MemberType;

                    if (memberType == typeof(char) || memberType == typeof(char?))
                    {
                        il.EmitCall(OpCodes.Call, typeof(SqlMapper).GetMethod(
                            memberType == typeof(char) ? nameof(SqlMapper.ReadChar) : nameof(SqlMapper.ReadNullableChar),
                            BindingFlags.Static | BindingFlags.Public), null); // stack is now [target][target][typed-value]
                    }
                    else
                    {
                        il.Emit(OpCodes.Dup); // stack is now [target][target][value][value]
                        il.Emit(OpCodes.Isinst, typeof(DBNull)); // stack is now [target][target][value-as-object][DBNull or null]
                        il.Emit(OpCodes.Brtrue_S, isDbNullLabel); // stack is now [target][target][value-as-object]

                        // unbox nullable enums as the primitive, i.e. byte etc

                        var nullUnderlyingType = Nullable.GetUnderlyingType(memberType);
                        var unboxType = nullUnderlyingType?.IsEnum() == true ? nullUnderlyingType : memberType;

                        if (unboxType.IsEnum())
                        {
                            var numericType = Enum.GetUnderlyingType(unboxType);
                            if (colType == typeof(string))
                            {
                                if (enumDeclareLocal == -1)
                                {
                                    enumDeclareLocal = il.DeclareLocal(typeof(string)).LocalIndex;
                                }

                                il.Emit(OpCodes.Castclass, typeof(string)); // stack is now [target][target][string]
                                StoreLocal(il, enumDeclareLocal); // stack is now [target][target]
                                il.Emit(OpCodes.Ldtoken, unboxType); // stack is now [target][target][enum-type-token]
                                il.EmitCall(OpCodes.Call, typeof(Type).GetMethod(nameof(Type.GetTypeFromHandle)),
                                    null); // stack is now [target][target][enum-type]
                                LoadLocal(il, enumDeclareLocal); // stack is now [target][target][enum-type][string]
                                il.Emit(OpCodes.Ldc_I4_1); // stack is now [target][target][enum-type][string][true]
                                il.EmitCall(OpCodes.Call, enumParse, null); // stack is now [target][target][enum-as-object]
                                il.Emit(OpCodes.Unbox_Any, unboxType); // stack is now [target][target][typed-value]
                            }
                            else
                            {
                                FlexibleConvertBoxedFromHeadOfStack(il, colType, unboxType, numericType);
                            }

                            if (nullUnderlyingType != null)
                            {
                                il.Emit(OpCodes.Newobj, memberType.GetConstructor(new[] { nullUnderlyingType })); // stack is now [target][target][typed-value]
                            }
                        }
                        else if (memberType.FullName == TypeProvider.LinqBinary)
                        {
                            il.Emit(OpCodes.Unbox_Any, typeof(byte[])); // stack is now [target][target][byte-array]
                            il.Emit(OpCodes.Newobj, memberType.GetConstructor(new Type[] { typeof(byte[]) })); // stack is now [target][target][binary]
                        }
                        else
                        {
                            TypeCode dataTypeCode = TypeExtensions.GetTypeCode(colType), unboxTypeCode = TypeExtensions.GetTypeCode(unboxType);
                            bool hasTypeHandler;
                            if ((hasTypeHandler = TypeProvider.ContainsHandler(unboxType)) || colType == unboxType || dataTypeCode == unboxTypeCode ||
                                dataTypeCode == TypeExtensions.GetTypeCode(nullUnderlyingType))
                            {
                                if (hasTypeHandler)
                                {
                                    il.EmitCall(OpCodes.Call,
                                        typeof(TypeHandlerCache<>).MakeGenericType(unboxType).GetMethod(nameof(TypeHandlerCache<int>.Parse)),
                                        null); // stack is now [target][target][typed-value]
                                }
                                else
                                {
                                    il.Emit(OpCodes.Unbox_Any, unboxType); // stack is now [target][target][typed-value]
                                }
                            }
                            else
                            {
                                // not a direct match; need to tweak the unbox
                                FlexibleConvertBoxedFromHeadOfStack(il, colType, nullUnderlyingType ?? unboxType, null);
                                if (nullUnderlyingType != null)
                                {
                                    il.Emit(OpCodes.Newobj,
                                        unboxType.GetConstructor(new[] { nullUnderlyingType })); // stack is now [target][target][typed-value]
                                }
                            }
                        }
                    }

                    if (specializedConstructor == null)
                    {
                        // Store the value in the property/field
                        if (item.Property != null)
                        {
                            il.Emit(type.IsValueType() ? OpCodes.Call : OpCodes.Callvirt, DefaultTypeMap.GetPropertySetter(item.Property, type));
                        }
                        else
                        {
                            il.Emit(OpCodes.Stfld, item.Field); // stack is now [target]
                        }
                    }

                    il.Emit(OpCodes.Br_S, finishLabel); // stack is now [target]

                    il.MarkLabel(isDbNullLabel); // incoming stack: [target][target][value]
                    if (specializedConstructor != null)
                    {
                        il.Emit(OpCodes.Pop);
                        if (item.MemberType.IsValueType())
                        {
                            var localIndex = il.DeclareLocal(item.MemberType).LocalIndex;
                            LoadLocalAddress(il, localIndex);
                            il.Emit(OpCodes.Initobj, item.MemberType);
                            LoadLocal(il, localIndex);
                        }
                        else
                        {
                            il.Emit(OpCodes.Ldnull);
                        }
                    }
                    else
                    {
                        il.Emit(OpCodes.Pop); // stack is now [target][target]
                        il.Emit(OpCodes.Pop); // stack is now [target]
                    }

                    if (first && returnNullIfFirstMissing)
                    {
                        il.Emit(OpCodes.Pop);
                        il.Emit(OpCodes.Ldnull); // stack is now [null]
                        il.Emit(OpCodes.Stloc_1);
                        il.Emit(OpCodes.Br, allDone);
                    }

                    il.MarkLabel(finishLabel);
                }

                first = false;
                index++;
            }

            if (type.IsValueType())
            {
                il.Emit(OpCodes.Pop);
            }
            else
            {
                if (specializedConstructor != null)
                {
                    il.Emit(OpCodes.Newobj, specializedConstructor);
                }

                il.Emit(OpCodes.Stloc_1); // stack is empty
            }

            il.MarkLabel(allDone);
            il.BeginCatchBlock(typeof(Exception)); // stack is Exception
            il.Emit(OpCodes.Ldloc_0); // stack is Exception, index
            il.Emit(OpCodes.Ldarg_0); // stack is Exception, index, reader
            LoadLocal(il, valueCopyLocal); // stack is Exception, index, reader, value
            il.EmitCall(OpCodes.Call, typeof(SqlMapper).GetMethod(nameof(SqlMapper.ThrowDataException)), null);
            il.EndExceptionBlock();

            il.Emit(OpCodes.Ldloc_1); // stack is [rval]
            if (type.IsValueType())
            {
                il.Emit(OpCodes.Box, type);
            }

            il.Emit(OpCodes.Ret);

            var funcType = System.Linq.Expressions.Expression.GetFuncType(typeof(IDataReader), returnType);
            return (Func<IDataReader, object>)dm.CreateDelegate(funcType);
        }

        private static void FlexibleConvertBoxedFromHeadOfStack(ILGenerator il, Type from, Type to, Type via)
        {
            MethodInfo op;
            if (from == (via ?? to))
            {
                il.Emit(OpCodes.Unbox_Any, to); // stack is now [target][target][typed-value]
            }
            else if ((op = GetOperator(from, to)) != null)
            {
                // this is handy for things like decimal <===> double
                il.Emit(OpCodes.Unbox_Any, from); // stack is now [target][target][data-typed-value]
                il.Emit(OpCodes.Call, op); // stack is now [target][target][typed-value]
            }
            else
            {
                var handled = false;
                var opCode = default(OpCode);
                switch (TypeExtensions.GetTypeCode(from))
                {
                    case TypeCode.Boolean:
                    case TypeCode.Byte:
                    case TypeCode.SByte:
                    case TypeCode.Int16:
                    case TypeCode.UInt16:
                    case TypeCode.Int32:
                    case TypeCode.UInt32:
                    case TypeCode.Int64:
                    case TypeCode.UInt64:
                    case TypeCode.Single:
                    case TypeCode.Double:
                        handled = true;
                        switch (TypeExtensions.GetTypeCode(via ?? to))
                        {
                            case TypeCode.Byte:
                                opCode = OpCodes.Conv_Ovf_I1_Un;
                                break;
                            case TypeCode.SByte:
                                opCode = OpCodes.Conv_Ovf_I1;
                                break;
                            case TypeCode.UInt16:
                                opCode = OpCodes.Conv_Ovf_I2_Un;
                                break;
                            case TypeCode.Int16:
                                opCode = OpCodes.Conv_Ovf_I2;
                                break;
                            case TypeCode.UInt32:
                                opCode = OpCodes.Conv_Ovf_I4_Un;
                                break;
                            case TypeCode.Boolean: // boolean is basically an int, at least at this level
                            case TypeCode.Int32:
                                opCode = OpCodes.Conv_Ovf_I4;
                                break;
                            case TypeCode.UInt64:
                                opCode = OpCodes.Conv_Ovf_I8_Un;
                                break;
                            case TypeCode.Int64:
                                opCode = OpCodes.Conv_Ovf_I8;
                                break;
                            case TypeCode.Single:
                                opCode = OpCodes.Conv_R4;
                                break;
                            case TypeCode.Double:
                                opCode = OpCodes.Conv_R8;
                                break;
                            default:
                                handled = false;
                                break;
                        }

                        break;
                }

                if (handled)
                {
                    il.Emit(OpCodes.Unbox_Any, from); // stack is now [target][target][col-typed-value]
                    il.Emit(opCode); // stack is now [target][target][typed-value]
                    if (to == typeof(bool))
                    {
                        // compare to zero; I checked "csc" - this is the trick it uses; nice
                        il.Emit(OpCodes.Ldc_I4_0);
                        il.Emit(OpCodes.Ceq);
                        il.Emit(OpCodes.Ldc_I4_0);
                        il.Emit(OpCodes.Ceq);
                    }
                }
                else
                {
                    il.Emit(OpCodes.Ldtoken, via ?? to); // stack is now [target][target][value][member-type-token]
                    il.EmitCall(OpCodes.Call, typeof(Type).GetMethod(nameof(Type.GetTypeFromHandle)), null); // stack is now [target][target][value][member-type]
                    il.EmitCall(OpCodes.Call, typeof(Convert).GetMethod(nameof(Convert.ChangeType), new Type[] { typeof(object), typeof(Type) }), null); // stack is now [target][target][boxed-member-type-value]
                    il.Emit(OpCodes.Unbox_Any, to); // stack is now [target][target][typed-value]
                }
            }
        }

        private static MethodInfo GetOperator(Type from, Type to)
        {
            if (to == null) return null;
            MethodInfo[] fromMethods, toMethods;
            return ResolveOperator(fromMethods = from.GetMethods(BindingFlags.Static | BindingFlags.Public), from, to, "op_Implicit")
                   ?? ResolveOperator(toMethods = to.GetMethods(BindingFlags.Static | BindingFlags.Public), from, to, "op_Implicit")
                   ?? ResolveOperator(fromMethods, from, to, "op_Explicit")
                   ?? ResolveOperator(toMethods, from, to, "op_Explicit");
        }

        private static MethodInfo ResolveOperator(MethodInfo[] methods, Type from, Type to, string name)
        {
            for (var i = 0; i < methods.Length; i++)
            {
                if (methods[i].Name != name || methods[i].ReturnType != to) continue;
                var args = methods[i].GetParameters();
                if (args.Length != 1 || args[0].ParameterType != from) continue;
                return methods[i];
            }

            return null;
        }

        private static void LoadLocal(ILGenerator il, int index)
        {
            if (index < 0 || index >= short.MaxValue) throw new ArgumentNullException(nameof(index));
            switch (index)
            {
                case 0:
                    il.Emit(OpCodes.Ldloc_0);
                    break;
                case 1:
                    il.Emit(OpCodes.Ldloc_1);
                    break;
                case 2:
                    il.Emit(OpCodes.Ldloc_2);
                    break;
                case 3:
                    il.Emit(OpCodes.Ldloc_3);
                    break;
                default:
                    if (index <= 255)
                    {
                        il.Emit(OpCodes.Ldloc_S, (byte)index);
                    }
                    else
                    {
                        il.Emit(OpCodes.Ldloc, (short)index);
                    }

                    break;
            }
        }

        private static void StoreLocal(ILGenerator il, int index)
        {
            if (index < 0 || index >= short.MaxValue) throw new ArgumentNullException(nameof(index));
            switch (index)
            {
                case 0:
                    il.Emit(OpCodes.Stloc_0);
                    break;
                case 1:
                    il.Emit(OpCodes.Stloc_1);
                    break;
                case 2:
                    il.Emit(OpCodes.Stloc_2);
                    break;
                case 3:
                    il.Emit(OpCodes.Stloc_3);
                    break;
                default:
                    if (index <= 255)
                    {
                        il.Emit(OpCodes.Stloc_S, (byte)index);
                    }
                    else
                    {
                        il.Emit(OpCodes.Stloc, (short)index);
                    }

                    break;
            }
        }

        private static void LoadLocalAddress(ILGenerator il, int index)
        {
            if (index < 0 || index >= short.MaxValue) throw new ArgumentNullException(nameof(index));

            if (index <= 255)
            {
                il.Emit(OpCodes.Ldloca_S, (byte)index);
            }
            else
            {
                il.Emit(OpCodes.Ldloca, (short)index);
            }
        }

        /// <summary>
        /// Throws a data exception, only used internally
        /// </summary>
        /// <param name="ex">The exception to throw.</param>
        /// <param name="index">The index the exception occured at.</param>
        /// <param name="reader">The reader the exception occured in.</param>
        /// <param name="value">The value that caused the exception.</param>
        public static void ThrowDataException(Exception ex, int index, IDataReader reader, object value)
        {
            Exception toThrow;
            try
            {
                string name = "(n/a)", formattedValue = "(n/a)";
                if (reader != null && index >= 0 && index < reader.FieldCount)
                {
                    name = reader.GetName(index);
                    try
                    {
                        if (value == null || value is DBNull)
                        {
                            formattedValue = "<null>";
                        }
                        else
                        {
                            formattedValue = Convert.ToString(value) + " - " + TypeExtensions.GetTypeCode(value.GetType());
                        }
                    }
                    catch (Exception valEx)
                    {
                        formattedValue = valEx.Message;
                    }
                }

                toThrow = new DataException($"Error parsing column {index} ({name}={formattedValue})", ex);
            }
            catch
            {
                // throw the **original** exception, wrapped as DataException
                toThrow = new DataException(ex.Message, ex);
            }

            throw toThrow;
        }

        private static void EmitInt32(ILGenerator il, int value)
        {
            switch (value)
            {
                case -1:
                    il.Emit(OpCodes.Ldc_I4_M1);
                    break;
                case 0:
                    il.Emit(OpCodes.Ldc_I4_0);
                    break;
                case 1:
                    il.Emit(OpCodes.Ldc_I4_1);
                    break;
                case 2:
                    il.Emit(OpCodes.Ldc_I4_2);
                    break;
                case 3:
                    il.Emit(OpCodes.Ldc_I4_3);
                    break;
                case 4:
                    il.Emit(OpCodes.Ldc_I4_4);
                    break;
                case 5:
                    il.Emit(OpCodes.Ldc_I4_5);
                    break;
                case 6:
                    il.Emit(OpCodes.Ldc_I4_6);
                    break;
                case 7:
                    il.Emit(OpCodes.Ldc_I4_7);
                    break;
                case 8:
                    il.Emit(OpCodes.Ldc_I4_8);
                    break;
                default:
                    if (value >= -128 && value <= 127)
                    {
                        il.Emit(OpCodes.Ldc_I4_S, (sbyte)value);
                    }
                    else
                    {
                        il.Emit(OpCodes.Ldc_I4, value);
                    }

                    break;
            }
        }

        internal static IEqualityComparer<string> connectionStringComparer = StringComparer.Ordinal;
    }
}