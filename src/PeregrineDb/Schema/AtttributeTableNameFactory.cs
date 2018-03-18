namespace PeregrineDb.Schema
{
    using System;
    using System.ComponentModel.DataAnnotations.Schema;
    using System.Reflection;
    using PeregrineDb.Dialects;

    /// <summary>
    /// If class has <see cref="TableAttribute"/> then it returns <see cref="TableAttribute.Name"/> without manipulating it. Otherwise;
    /// - Removes the specified suffix, if any (so classes can be called e.g. UserEntity)
    /// - Converts the class name into snake_case
    /// </summary>
    public class AtttributeTableNameFactory
        : ITableNameFactory
    {
        protected const string DefaultSuffix = "Entity";
        private readonly string suffix;

        public AtttributeTableNameFactory(string suffix = DefaultSuffix)
        {
            this.suffix = suffix;
        }

        /// <inheritdoc/>
        public string GetTableName(Type type, IDialect dialect)
        {
            var tableAttribute = type.GetTypeInfo().GetCustomAttribute<TableAttribute>(false);
            return tableAttribute != null
                ? GetTableNameFromAttribute(dialect, tableAttribute)
                : dialect.MakeTableName(this.GetTableNameFromType(type));
        }

        /// <summary>
        /// Gets the table name from the given type.
        /// By default, pluralizes and removes the interface "I" prefix.
        /// </summary>
        protected virtual string GetTableNameFromType(Type type)
        {
            return type.Name + "s";
        }

        private static string GetTableNameFromAttribute(IDialect dialect, TableAttribute tableAttribute)
        {
            return string.IsNullOrEmpty(tableAttribute.Schema)
                ? dialect.MakeTableName(tableAttribute.Name)
                : dialect.MakeTableName(tableAttribute.Schema, tableAttribute.Name);
        }

        protected string RemoveSuffix(string value)
        {
            if (this.suffix == null)
            {
                return value;
            }

            var length = value.Length - this.suffix.Length;
            if (length <= 0)
            {
                return value;
            }

            return value.EndsWith(this.suffix, StringComparison.OrdinalIgnoreCase)
                ? value.Substring(0, length)
                : value;
        }
    }
}