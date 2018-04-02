namespace PeregrineDb.Schema
{
    using System;

    /// <summary>
    /// Defines how to get the table name from a specific type.
    /// </summary>
    public interface ITableNameConvention
    {
        /// <summary>
        /// Gets the table name from the <paramref name="type"/>.
        /// The table name should be properly escaped
        /// (Probably by calling <see cref="ISqlNameEscaper.EscapeTableName(string)"/>.
        /// </summary>
        string GetTableName(Type type);
    }
}