
namespace PeregrineDb.Mapping
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using PeregrineDb.Databases.Mapper;
#if NETSTANDARD1_3
    using ApplicationException = System.InvalidOperationException;

#endif

    /// <summary>
    /// A bag of parameters that can be passed to the Dapper Query and Execute methods
    /// </summary>
    internal class DynamicParameters
        : IDynamicParameters
    {
        internal const DbType EnumerableMultiParameter = (DbType)(-1);
        private static readonly Dictionary<Identity, Action<IDbCommand, object>> paramReaderCache = new Dictionary<Identity, Action<IDbCommand, object>>();
        private readonly Dictionary<string, ParamInfo> parameters = new Dictionary<string, ParamInfo>();
        private List<object> templates;

        object this[string name] => this.parameters.TryGetValue(name, out var param) ? param.Value : null;

        /// <summary>
        /// construct a dynamic parameter bag
        /// </summary>
        public DynamicParameters()
        {
            this.RemoveUnused = true;
        }

        /// <summary>
        /// construct a dynamic parameter bag
        /// </summary>
        /// <param name="template">can be an anonymous type or a DynamicParameters bag</param>
        public DynamicParameters(object template)
        {
            this.RemoveUnused = true;
            this.AddDynamicParams(template);
        }

        /// <summary>
        /// Append a whole object full of params to the dynamic
        /// EG: AddDynamicParams(new {A = 1, B = 2}) // will add property A and B to the dynamic
        /// </summary>
        /// <param name="param"></param>
        public void AddDynamicParams(object param)
        {
            if (param == null)
            {
                return;
            }

            if (param is DynamicParameters subDynamic)
            {
                if (subDynamic.parameters != null)
                {
                    foreach (var kvp in subDynamic.parameters)
                    {
                        this.parameters.Add(kvp.Key, kvp.Value);
                    }
                }

                if (subDynamic.templates != null)
                {
                    this.templates = this.templates ?? new List<object>();
                    foreach (var t in subDynamic.templates)
                    {
                        this.templates.Add(t);
                    }
                }
            }
            else
            {
                if (param is IEnumerable<KeyValuePair<string, object>> dictionary)
                {
                    foreach (var kvp in dictionary)
                    {
                        this.Add(kvp.Key, kvp.Value);
                    }
                }
                else
                {
                    this.templates = this.templates ?? new List<object>();
                    this.templates.Add(param);
                }
            }
        }

        /// <summary>
        /// Add a parameter to this dynamic parameter list.
        /// </summary>
        /// <param name="name">The name of the parameter.</param>
        /// <param name="value">The value of the parameter.</param>
        /// <param name="dbType">The type of the parameter.</param>
        /// <param name="direction">The in or out direction of the parameter.</param>
        /// <param name="size">The size of the parameter.</param>
        /// <param name="precision">The precision of the parameter.</param>
        /// <param name="scale">The scale of the parameter.</param>
        public void Add(
            string name,
            object value = null,
            DbType? dbType = null,
            ParameterDirection? direction = null,
            int? size = null,
            byte? precision = null,
            byte? scale = null)
        {
            this.parameters[Clean(name)] = new ParamInfo
                {
                    Name = name,
                    Value = value,
                    ParameterDirection = direction ?? ParameterDirection.Input,
                    DbType = dbType,
                    Size = size,
                    Precision = precision,
                    Scale = scale
                };
        }

        private static string Clean(string name)
        {
            if (!string.IsNullOrEmpty(name))
            {
                switch (name[0])
                {
                    case '@':
                    case ':':
                    case '?':
                        return name.Substring(1);
                }
            }

            return name;
        }

        public void AddParameters(IDbCommand command, Identity identity)
        {
            if (this.templates != null)
            {
                foreach (var template in this.templates)
                {
                    var newIdent = identity.ForDynamicParameters(template.GetType());
                    Action<IDbCommand, object> appender;

                    lock (paramReaderCache)
                    {
                        if (!paramReaderCache.TryGetValue(newIdent, out appender))
                        {
                            appender = TypeMapper.CreateParamInfoGenerator(newIdent, true, this.RemoveUnused);
                            paramReaderCache[newIdent] = appender;
                        }
                    }

                    appender(command, template);
                }

                // The parameters were added to the command, but not the
                // DynamicParameters until now.
                foreach (IDbDataParameter param in command.Parameters)
                {
                    // If someone makes a DynamicParameters with a template,
                    // then explicitly adds a parameter of a matching name,
                    // it will already exist in 'parameters'.
                    if (!this.parameters.ContainsKey(param.ParameterName))
                    {
                        this.parameters.Add(param.ParameterName, new ParamInfo
                            {
                                AttachedParam = param,
                                CameFromTemplate = true,
                                DbType = param.DbType,
                                Name = param.ParameterName,
                                ParameterDirection = param.Direction,
                                Size = param.Size,
                                Value = param.Value
                            });
                    }
                }
            }

            foreach (var param in this.parameters.Values)
            {
                if (param.CameFromTemplate)
                {
                    continue;
                }

                var val = param.Value;
                var name = Clean(param.Name);

                if (val is ICustomQueryParameter customParameter)
                {
                    customParameter.AddParameter(command, name);
                }
                else
                {
                    var dbType = param.DbType;
                    IDbTypeConverter converter = null;
                    if (dbType == null && val != null)
                    {
                        dbType = TypeProvider.LookupDbType(val.GetType(), name, true, out converter);
                    }

                    if (dbType == EnumerableMultiParameter)
                    {
                        TypeMapper.PackListParameters(command, name, val);
                    }
                    else
                    {
                        var add = !command.Parameters.Contains(name);
                        IDbDataParameter p;
                        if (add)
                        {
                            p = command.CreateParameter();
                            p.ParameterName = name;
                        }
                        else
                        {
                            p = (IDbDataParameter)command.Parameters[name];
                        }

                        p.Direction = param.ParameterDirection;
                        if (converter == null)
                        {
                            p.Value = TypeMapper.SanitizeParameterValue(val);
                            if (dbType != null && p.DbType != dbType)
                            {
                                p.DbType = dbType.Value;
                            }

                            var s = val as string;
                            if (s?.Length <= DbString.DefaultLength)
                            {
                                p.Size = DbString.DefaultLength;
                            }

                            if (param.Size != null)
                            {
                                p.Size = param.Size.Value;
                            }

                            if (param.Precision != null)
                            {
                                p.Precision = param.Precision.Value;
                            }

                            if (param.Scale != null)
                            {
                                p.Scale = param.Scale.Value;
                            }
                        }
                        else
                        {
                            if (dbType != null)
                            {
                                p.DbType = dbType.Value;
                            }

                            if (param.Size != null)
                            {
                                p.Size = param.Size.Value;
                            }

                            if (param.Precision != null)
                            {
                                p.Precision = param.Precision.Value;
                            }

                            if (param.Scale != null)
                            {
                                p.Scale = param.Scale.Value;
                            }

                            converter.SetValue(p, val ?? DBNull.Value);
                        }

                        if (add)
                        {
                            command.Parameters.Add(p);
                        }

                        param.AttachedParam = p;
                    }
                }
            }
        }

        /// <summary>
        /// If true, the command-text is inspected and only values that are clearly used are included on the connection
        /// </summary>
        public bool RemoveUnused { get; set; }

        /// <summary>
        /// Get the value of a parameter
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="name"></param>
        /// <returns>The value, note DBNull.Value is not returned, instead the value is returned as null</returns>
        public T Get<T>(string name)
        {
            var paramInfo = this.parameters[Clean(name)];
            var attachedParam = paramInfo.AttachedParam;
            var val = attachedParam == null ? paramInfo.Value : attachedParam.Value;
            if (val != DBNull.Value)
            {
                return (T)val;
            }

            if (default(T) == null)
            {
                return default;
            }

            throw new ApplicationException(
                "Attempting to cast a DBNull to a non nullable type! Note that out/return parameters will not have updated values until the data stream completes (after the 'foreach' for Query(..., buffered: false), or after the GridReader has been disposed for QueryMultiple)");
        }

        private sealed class ParamInfo
        {
            public string Name { get; set; }

            public object Value { get; set; }

            public ParameterDirection ParameterDirection { get; set; }

            public DbType? DbType { get; set; }

            public int? Size { get; set; }

            public IDbDataParameter AttachedParam { get; set; }

            internal bool CameFromTemplate { get; set; }

            public byte? Precision { get; set; }

            public byte? Scale { get; set; }
        }
    }
}