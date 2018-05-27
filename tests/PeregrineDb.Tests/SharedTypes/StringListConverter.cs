namespace PeregrineDb.Tests.SharedTypes
{
    using System.Collections.Generic;
    using System.Data;
    using System.Linq;
    using PeregrineDb.Mapping;

    internal class StringListConverter
        : DbTypeConverter<List<string>>
    {
        private StringListConverter()
        {
        }

        public static readonly StringListConverter Default = new StringListConverter();

        //Just a simple List<string> type handler implementation
        public override void SetValue(IDbDataParameter parameter, List<string> value)
        {
            parameter.Value = string.Join(",", value);
        }

        public override void SetNullValue(IDbDataParameter parameter)
        {
            parameter.Value = string.Empty;
        }

        public override List<string> Parse(object value)
        {
            return ((value as string) ?? "").Split(',').ToList();
        }
    }
}