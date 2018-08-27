// <copyright file="DefaultTypeMap.cs" company="Berkeleybross">
// Copyright (c) Berkeleybross. All rights reserved.
// </copyright>

namespace PeregrineDb.Databases.Mapper
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Reflection;
    using PeregrineDb.Utils;

    /// <summary>
    /// Represents default type mapping strategy used by Dapper
    /// </summary>
    internal sealed class DefaultTypeMap
    {
        public DefaultTypeMap(Type type)
        {
            Ensure.NotNull(type, nameof(type));

            this.Properties = type.GetProperties(BindingFlags.Public | BindingFlags.Instance).Where(p => p.GetSetMethod(false) != null).ToList();
        }

        /// <summary>
        /// Gets or sets a value indicating whether column names like User_Id should be allowed to match properties/fields like UserId.
        /// </summary>
        public static bool MatchNamesWithUnderscores { get; set; } = true;

        /// <summary>
        /// Gets the settable properties for this typemap.
        /// </summary>
        public List<PropertyInfo> Properties { get; }

        /// <summary>
        /// Gets member mapping for column
        /// </summary>
        /// <param name="columnName">DataReader column name</param>
        /// <returns>Mapping implementation</returns>
        public PropertyInfo GetMember(string columnName)
        {
            var property = this.Properties.Find(p => string.Equals(p.Name, columnName, StringComparison.Ordinal))
                           ?? this.Properties.Find(p => string.Equals(p.Name, columnName, StringComparison.OrdinalIgnoreCase));

            if (property != null || !MatchNamesWithUnderscores)
            {
                return property;
            }

            var alternateColumnName = columnName.Replace("_", string.Empty);
            return this.Properties.Find(p => string.Equals(p.Name, alternateColumnName, StringComparison.Ordinal))
                   ?? this.Properties.Find(p => string.Equals(p.Name, alternateColumnName, StringComparison.OrdinalIgnoreCase));
        }
    }
}
