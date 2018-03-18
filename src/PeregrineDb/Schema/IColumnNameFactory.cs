namespace PeregrineDb.Schema
{
    /// <summary>
    /// Defines how to get the column name from a specific property.
    /// </summary>
    public interface IColumnNameFactory
    {
        /// <summary>
        /// Gets the (unescaped) name of the column from the <paramref name="property"/>.
        /// </summary>
        string GetColumnName(PropertySchema property);
    }
}