namespace PeregrineDb.Schema
{
    using System;
    using System.Reflection;

    /// <summary>
    /// Represents a column in the databaseConnection that is checked for equality with a property.
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
            return propertyValue == null || propertyValue is DBNull;
        }
    }
}