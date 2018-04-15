namespace PeregrineDb.Databases.Mapper
{
    using System;
    using System.Data;

    /// <summary>
    /// Base-class for simple type-handlers
    /// </summary>
    /// <typeparam name="T">This <see cref="Type"/> this handler is for.</typeparam>
    internal abstract class TypeHandler<T>
        : ITypeHandler
    {
        /// <summary>
        /// Assign the value of a parameter before a command executes
        /// </summary>
        /// <param name="parameter">The parameter to configure</param>
        /// <param name="value">Parameter value</param>
        public abstract void SetValue(IDbDataParameter parameter, T value);

        /// <summary>
        /// Parse a database value back to a typed value
        /// </summary>
        /// <param name="value">The value from the database</param>
        /// <returns>The typed value</returns>
        public abstract T Parse(object value);

        void ITypeHandler.SetValue(IDbDataParameter parameter, object value)
        {
            if (value is DBNull)
            {
                parameter.Value = value;
            }
            else
            {
                this.SetValue(parameter, (T)value);
            }
        }

        object ITypeHandler.Parse(Type destinationType, object value)
        {
            return this.Parse(value);
        }
    }
}
