// <copyright file="DbTypeConverter.cs" company="Berkeleybross">
// Copyright (c) Berkeleybross. All rights reserved.
// </copyright>

namespace PeregrineDb.Mapping
{
    using System;
    using System.Data;

    /// <summary>
    /// Base-class for simple type-handlers
    /// </summary>
    /// <typeparam name="T">This <see cref="Type"/> this handler is for.</typeparam>
    public abstract class DbTypeConverter<T>
        : IDbTypeConverter
    {
        /// <summary>
        /// Assign the value of a parameter before a command executes
        /// </summary>
        /// <param name="parameter">The parameter to configure</param>
        /// <param name="value">Parameter value</param>
        public abstract void SetValue(IDbDataParameter parameter, T value);

        public abstract void SetNullValue(IDbDataParameter parameter);

        /// <summary>
        /// Parse a database value back to a typed value
        /// </summary>
        /// <param name="value">The value from the database</param>
        /// <returns>The typed value</returns>
        public abstract T Parse(object value);

        void IDbTypeConverter.SetValue(IDbDataParameter parameter, object value)
        {
            if (Convert.IsDBNull(value))
            {
                this.SetNullValue(parameter);
            }
            else
            {
                this.SetValue(parameter, (T)value);
            }
        }

        object IDbTypeConverter.Parse(Type destinationType, object value)
        {
            return this.Parse(value);
        }
    }
}
