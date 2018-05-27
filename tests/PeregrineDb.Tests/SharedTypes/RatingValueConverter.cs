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

        public override void SetValue(IDbDataParameter parameter, RatingValue value)
        {
            // ... null, range checks etc ...
            parameter.DbType = DbType.Int32;
            parameter.Value = value.Value;
        }

        public override void SetNullValue(IDbDataParameter parameter)
        {
            parameter.DbType = DbType.Int32;
            parameter.Value = DBNull.Value;
        }

        public override RatingValue Parse(object value)
        {
            if (value is int i)
            {
                return new RatingValue { Value = i };
            }

            if (Convert.IsDBNull(value))
            {
                return null;
            }

            throw new FormatException("Invalid conversion to RatingValue");
        }
    }

    public class RatingValue
    {
        public int Value { get; set; }
    }
}
