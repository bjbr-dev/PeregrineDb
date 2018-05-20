using System;

namespace PeregrineDb.Tests.SharedTypes
{
    using System.Data;
    using PeregrineDb.Mapping;

    internal class RatingValueConverter
        : DbTypeConverter<RatingValue>
    {
        private RatingValueConverter()
        {
        }

        public static readonly RatingValueConverter Default = new RatingValueConverter();

        public override RatingValue Parse(object value)
        {
            if (value is int)
            {
                return new RatingValue() { Value = (int)value };
            }

            throw new FormatException("Invalid conversion to RatingValue");
        }

        public override void SetValue(IDbDataParameter parameter, RatingValue value)
        {
            // ... null, range checks etc ...
            parameter.DbType = System.Data.DbType.Int32;
            parameter.Value = value.Value;
        }
    }

    public class RatingValue
    {
        public int Value { get; set; }
    }
}
