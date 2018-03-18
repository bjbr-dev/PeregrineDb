namespace PeregrineDb.Schema
{
    using System.ComponentModel.DataAnnotations.Schema;

    /// <summary>
    /// Default implementation of a column name factory.
    /// Uses the <see cref="ColumnAttribute"/> if present, otherwise pluralizes class name.
    /// </summary>
    public class AttributeColumnNameFactory
        : IColumnNameFactory
    {
        /// <inheritdoc />
        public string GetColumnName(PropertySchema property)
        {
            var columnAttribute = property.FindAttribute<ColumnAttribute>();
            return columnAttribute != null
                ? columnAttribute.Name
                : this.GetColumnNameFromType(property);
        }

        /// <summary>
        /// Gets the table name from the given type.
        /// By default, pluralizes and removes the interface "I" prefix.
        /// </summary>
        protected virtual string GetColumnNameFromType(PropertySchema property)
        {
            return property.Name;
        }
    }
}