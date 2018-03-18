namespace PeregrineDb.Schema
{
    using System;
    using System.ComponentModel.DataAnnotations.Schema;

    /// <summary>
    /// Implements the <see cref="ITableNameFactory"/> by using the <see cref="TableAttribute"/> if present,
    /// otherwise takes the class name, removes the suffix (if present) and pluralizes it.
    /// </summary>
    public class SuffixRemovingTableNameFactory
        : DefaultTableNameFactory
    {
        private readonly string suffix;

        /// <summary>
        /// Initializes a new instance of the <see cref="SuffixRemovingTableNameFactory"/> class.
        /// </summary>
        public SuffixRemovingTableNameFactory(string suffix)
        {
            this.suffix = suffix;
        }

        /// <inheritdoc />
        protected override string GetTableNameFromType(Type type)
        {
            var name = type.Name;

            if (name.EndsWith(this.suffix, StringComparison.OrdinalIgnoreCase))
            {
                var length = name.Length - this.suffix.Length;
                if (length > 0)
                {
                    name = name.Substring(0, length);
                }
            }

            return name + "s";
        }
    }
}