namespace PeregrineDb.Schema
{
    using System;
    using System.Linq;
    using System.Reflection;

    /// <summary>
    /// Stores data useful for building a <see cref="ColumnSchema"/>.
    /// </summary>
    public class PropertySchema
    {
        /// <summary>
        /// Gets or sets the name of the property
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// Gets or sets the original <see cref="PropertyInfo"/> used to create this builder.
        /// NB: To get an attribute efficiently, use the <see cref="FindAttribute{T}"/> method.
        /// </summary>
        public PropertyInfo PropertyInfo { get; set; }

        /// <summary>
        /// Gets or sets the custom attributes on the property.
        /// NB: To get an attribute efficiently, use the <see cref="FindAttribute{T}"/> method.
        /// </summary>
        public Attribute[] CustomAttributes { get; set; }

        /// <summary>
        /// Gets or sets the type of the property.
        /// </summary>
        public Type Type { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether the type of the property is a Nullable&lt;T&gt;
        /// </summary>
        public bool IsNullable { get; set; }

        /// <summary>
        /// Gets or sets the effective type of the property -
        /// </summary>
        public Type EffectiveType { get; set; }

        /// <summary>
        /// Creates a <see cref="PropertySchema"/> from the <paramref name="property"/>.
        /// </summary>
        public static PropertySchema MakePropertySchema(PropertyInfo property)
        {
            var type = property.PropertyType;
            var underlyingType = Nullable.GetUnderlyingType(type);

            return new PropertySchema
                {
                    CustomAttributes = property.GetCustomAttributes(false).Cast<Attribute>().ToArray(),
                    Name = property.Name,
                    PropertyInfo = property,
                    Type = type,
                    EffectiveType = underlyingType ?? type,
                    IsNullable = underlyingType != null
                };
        }

        /// <summary>
        /// Gets the first attribute of type T or null
        /// </summary>
        public T FindAttribute<T>()
            where T : Attribute
        {
            var attributes = this.CustomAttributes;

            // ReSharper disable once ForCanBeConvertedToForeach
            // PERF - Called several times in a tight loop
            for (var i = 0; i < attributes.Length; i++)
            {
                if (attributes[i] is T result)
                {
                    return result;
                }
            }

            return null;
        }
    }
}