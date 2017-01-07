// <copyright file="ConditionColumnSchema.cs" company="Berkeleybross">
// Copyright (c) Berkeleybross. All rights reserved.
// </copyright>
namespace Dapper.MicroCRUD.Schema
{
    using System;
    using System.Reflection;

    /// <summary>
    /// Represents a column in the database that is checked for equality with a property.
    /// </summary>
    public class ConditionColumnSchema
    {
        private readonly PropertyInfo propertyInfo;

        /// <summary>
        /// Initializes a new instance of the <see cref="ConditionColumnSchema"/> class.
        /// </summary>
        public ConditionColumnSchema(ColumnSchema column, PropertyInfo propertyInfo)
        {
            this.Column = column;
            this.propertyInfo = propertyInfo;
        }

        /// <summary>
        /// Gets the column being compared to
        /// </summary>
        public ColumnSchema Column { get; }

        /// <summary>
        /// Checks whether the property being compared is currently null.
        /// </summary>
        public bool IsNull(object conditions)
        {
            var propertyValue = this.propertyInfo.GetValue(conditions);
            return propertyValue == null || Convert.IsDBNull(propertyValue);
        }
    }
}