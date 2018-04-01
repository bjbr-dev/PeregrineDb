namespace PeregrineDb.Schema
{
    /// <summary>
    /// Represents a column in a <see cref="TableSchema"/>.
    /// </summary>
    public class ColumnSchema
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="ColumnSchema"/> class.
        /// </summary>
        public ColumnSchema(
            int index,
            string propertyName,
            string columnName,
            string selectName,
            ColumnUsage usage,
            DbTypeEx columnType)
        {
            this.Index = index;
            this.PropertyName = propertyName;
            this.ColumnName = columnName;
            this.SelectName = selectName;
            this.Usage = usage;
            this.ColumnType = columnType;
        }

        public int Index { get; }

        public string PropertyName { get; }

        /// <summary>
        /// Gets the name of the column in the databaseConnection
        /// </summary>
        public string ColumnName { get; }

        /// <summary>
        /// Gets the name of the column when being returned to Dapper so that it can be bound.
        /// This will usually be the same as <see cref="ColumnName"/>, unless the property has the [Column] attribute applied.
        /// </summary>
        public string SelectName { get; }

        /// <summary>
        /// Gets how this property should be used in various places.
        /// </summary>
        public ColumnUsage Usage { get; }

        /// <summary>
        /// Gets the type of this column
        /// </summary>
        public DbTypeEx ColumnType { get; }
    }
}