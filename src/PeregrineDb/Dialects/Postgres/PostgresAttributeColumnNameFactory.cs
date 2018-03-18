namespace PeregrineDb.Dialects.Postgres
{
    using System.ComponentModel.DataAnnotations.Schema;
    using System.Text;
    using PeregrineDb.Schema;

    /// <summary>
    /// If property has <see cref="ColumnAttribute"/> then it returns <see cref="ColumnAttribute.Name"/> without manipulating it.
    ///  Otherwise it converts the property name into snake_case
    /// </summary>
    public class PostgresAttributeColumnNameFactory
        : AttributeColumnNameFactory
    {
        protected override string GetColumnNameFromType(PropertySchema property)
        {
            return ToSnakeCase(property.Name);
        }

        internal static string ToSnakeCase(string pascalCaseString)
        {
            var builder = new StringBuilder(pascalCaseString.Length + 10);

            foreach (var character in pascalCaseString)
            {
                if (char.IsUpper(character))
                {
                    if (builder.Length > 0)
                    {
                        builder.Append("_");
                    }

                    builder.Append(char.ToLower(character));
                }
                else
                {
                    builder.Append(character);
                }
            }

            return builder.ToString();
        }
    }
}