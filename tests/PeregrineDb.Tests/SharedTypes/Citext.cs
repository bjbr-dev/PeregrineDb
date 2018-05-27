namespace PeregrineDb.Tests.SharedTypes
{
    using System;
    using System.Data;
    using Npgsql;
    using NpgsqlTypes;
    using PeregrineDb.Mapping;
    using PeregrineDb.Utils;

    public class Citext
    {
        public Citext(string value)
        {
            Ensure.NotNull(value, nameof(value));
            this.Value = value;
        }

        public string Value { get; }

        public static implicit operator Citext(string value)
        {
            if (value == null)
            {
                return null;
            }

            return new Citext(value);
        }
    }

    public class CitextConverter
        : DbTypeConverter<Citext>
    {
        public override void SetNullValue(IDbDataParameter parameter)
        {
            var npgsqlParameter = (NpgsqlParameter)parameter;
            npgsqlParameter.DbType = DbType.String;
            npgsqlParameter.NpgsqlDbType = NpgsqlDbType.Citext;
            parameter.Value = DBNull.Value;
        }

        public override void SetValue(IDbDataParameter parameter, Citext value)
        {
            var npgsqlParameter = (NpgsqlParameter)parameter;
            npgsqlParameter.DbType = DbType.String;
            npgsqlParameter.NpgsqlDbType = NpgsqlDbType.Citext;
            if (value != null)
            {
                parameter.Value = value.Value;
                parameter.Size = value.Value.Length;
            }
            else
            {
                parameter.Value = DBNull.Value;
            }
        }

        public override Citext Parse(object value)
        {
            if (value is DBNull || value == null)
            {
                return null;
            }

            return new Citext((string)value);
        }
    }
}