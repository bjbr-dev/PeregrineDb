// <copyright file="ColumnSchema.cs" company="Berkeleybross">
// Copyright (c) Berkeleybross. All rights reserved.
// </copyright>
namespace Dapper.MicroCRUD.Entities
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
            string columnName,
            string selectName,
            string parameterName,
            ColumnUsage usage)
        {
            this.ColumnName = columnName;
            this.SelectName = selectName;
            this.ParameterName = parameterName;
            this.Usage = usage;
        }

        /// <summary>
        /// Gets the name of the column in the database
        /// </summary>
        public string ColumnName { get; }

        /// <summary>
        /// Gets the name of the column when being returned to Dapper so that it can be bound.
        /// This will usually be the same as <see cref="ColumnName"/>, unless the property has the [Column] attribute applied.
        /// </summary>
        public string SelectName { get; }

        /// <summary>
        /// Gets the name of the column when used as a parameter. (Will be prefixed with an @ to signify it's a parameter).
        /// </summary>
        public string ParameterName { get; }

        /// <summary>
        /// Gets how this property should be used in various places.
        /// </summary>
        public ColumnUsage Usage { get; }
    }
}