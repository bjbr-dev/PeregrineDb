// <copyright file="DefaultColumnNameResolver.cs" company="Berkeleybross">
// Copyright (c) Berkeleybross. All rights reserved.
// </copyright>
namespace Dapper.MicroCRUD.Entities
{
    using System.ComponentModel.DataAnnotations.Schema;
    using System.Reflection;

    /// <summary>
    /// If the property has the <see cref="TableAttribute"/> defined then uses that name, otherwise takes the name of the property.
    /// </summary>
    public class DefaultColumnNameResolver
        : IColumnNameResolver
    {
        /// <inheritdoc/>
        public virtual string ResolveColumnName(PropertyInfo propertyInfo)
        {
            var columnAttribute = propertyInfo.GetCustomAttribute<ColumnAttribute>();

            return columnAttribute != null
                ? columnAttribute.Name
                : propertyInfo.Name;
        }
    }
}