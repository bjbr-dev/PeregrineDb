namespace PeregrineDb.Dialects.Postgres
{
    using System;
    using System.ComponentModel.DataAnnotations.Schema;
    using PeregrineDb.Schema;

    /// <summary>
    /// If class has <see cref="TableAttribute"/> then it returns <see cref="TableAttribute.Name"/> without manipulating it.
    ///  Otherwise it:
    /// - Removes the specified suffix, if any (so classes can be called e.g. DogEntity)
    /// - Converts the class name into lowercase snake_case
    /// - Escapes it using the <see cref="ISqlNameEscaper"/>
    /// </summary>
    public class PostgresAttributeTableNameConvention
        : AtttributeTableNameConvention
    {
        public PostgresAttributeTableNameConvention(ISqlNameEscaper nameEscaper, string suffix = DefaultSuffix)
            : base(nameEscaper, suffix)
        {
        }

        protected override string GetTableNameFromType(Type type)
        {
            var unescapedTableName = PostgresAttributeColumnNameConvention.ToSnakeCase(this.RemoveSuffix(type.Name));
            return this.NameEscaper.EscapeTableName(unescapedTableName);
        }
    }
}