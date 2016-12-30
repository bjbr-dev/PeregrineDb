// <copyright file="DefaultColumnNameResolver.cs" company="Berkeleybross">
// Copyright (c) Berkeleybross. All rights reserved.
// </copyright>
namespace Dapper.MicroCRUD.Entities
{
    using System.ComponentModel.DataAnnotations.Schema;
    using System.Reflection;
    using Dapper.MicroCRUD;

    /// <summary>
    /// If the property has the <see cref="TableAttribute"/> defined then uses that name, otherwise takes the name of the property.
    /// </summary>
    public class DefaultColumnNameResolver
        : IColumnNameResolver
    {
        /// <inheritdoc/>
        public virtual string ResolveColumnName(PropertyInfo propertyInfo, Dialect dialect)
        {
            var columnAttribute = propertyInfo.GetCustomAttribute<ColumnAttribute>();

            return dialect.EscapeMostReservedCharacters(columnAttribute != null
                ? columnAttribute.Name
                : propertyInfo.Name);
        }
    }
}