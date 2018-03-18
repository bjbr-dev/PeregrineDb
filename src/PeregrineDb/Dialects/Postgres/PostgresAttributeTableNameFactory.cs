namespace PeregrineDb.Dialects.Postgres
{
    using System;
    using System.ComponentModel.DataAnnotations.Schema;
    using PeregrineDb.Schema;

    /// <summary>
    /// If class has <see cref="TableAttribute"/> then it returns <see cref="TableAttribute.Name"/> without manipulating it.
    ///  Otherwise it:
    /// - Removes the specified suffix, if any (so classes can be called e.g. UserEntity)
    /// - Converts the class name into lowercase snake_case
    /// </summary>
    public class PostgresAttributeTableNameFactory
        : AtttributeTableNameFactory
    {
        public PostgresAttributeTableNameFactory(string suffix = DefaultSuffix)
            : base(suffix)
        {
        }

        protected override string GetTableNameFromType(Type type)
        {
            return PostgresAttributeColumnNameFactory.ToSnakeCase(this.RemoveSuffix(type.Name));
        }
    }
}