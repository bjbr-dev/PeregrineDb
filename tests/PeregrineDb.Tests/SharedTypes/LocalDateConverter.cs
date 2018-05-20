namespace PeregrineDb.Tests.SharedTypes
{
    using System;
    using System.Data;
    using PeregrineDb.Mapping;

    internal struct LocalDate
    {
        public int Year { get; set; }
        public int Month { get; set; }
        public int Day { get; set; }
    }

    internal class LocalDateConverter
        : DbTypeConverter<LocalDate>
    {
        private LocalDateConverter() { /* private constructor */ }

        // Make the field type ITypeHandler to ensure it cannot be used with SqlMapper.AddTypeHandler<T>(TypeHandler<T>)
        // by mistake.
        public static readonly IDbTypeConverter Default = new LocalDateConverter();

        public override LocalDate Parse(object value)
        {
            var date = (DateTime)value;
            return new LocalDate { Year = date.Year, Month = date.Month, Day = date.Day };
        }

        public override void SetValue(IDbDataParameter parameter, LocalDate value)
        {
            parameter.DbType = DbType.DateTime;
            parameter.Value = new DateTime(value.Year, value.Month, value.Day);
        }
    }
}