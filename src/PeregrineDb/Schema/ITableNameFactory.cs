namespace PeregrineDb.Schema
{
    using System;
    using PeregrineDb.Dialects;

    /// <summary>
    /// Defines how to get the table name from a specific type.
    /// </summary>
    public interface ITableNameFactory
    {
        /// <summary>
        /// Gets the table name from the <paramref name="type"/>.
        /// The table name should be properly escaped for the given <paramref name="dialect"/>
        /// (Probably by calling <see cref="IDialect.MakeTableName(string, string)"/>.
        /// </summary>
        string GetTableName(Type type, IDialect dialect);
    }
}