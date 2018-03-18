namespace PeregrineDb
{
    using System;
    using System.Collections.Generic;
    using System.Collections.Immutable;
    using System.Data;
    using PeregrineDb.Dialects;
    using PeregrineDb.Schema;

    public static class DefaultConfig
    {
        private static readonly ImmutableDictionary<Type, DbType> DefaultSqlTypeMapping = new Dictionary<Type, DbType>
            {
                [typeof(byte)] = DbType.Byte,
                [typeof(sbyte)] = DbType.SByte,
                [typeof(short)] = DbType.Int16,
                [typeof(ushort)] = DbType.UInt16,
                [typeof(int)] = DbType.Int32,
                [typeof(uint)] = DbType.UInt32,
                [typeof(long)] = DbType.Int64,
                [typeof(ulong)] = DbType.UInt64,
                [typeof(float)] = DbType.Single,
                [typeof(double)] = DbType.Double,
                [typeof(decimal)] = DbType.Decimal,
                [typeof(bool)] = DbType.Boolean,
                [typeof(string)] = DbType.String,
                [typeof(char)] = DbType.StringFixedLength,
                [typeof(Guid)] = DbType.Guid,
                [typeof(DateTime)] = DbType.DateTime,
                [typeof(DateTimeOffset)] = DbType.DateTimeOffset,
                [typeof(TimeSpan)] = DbType.Time,
                [typeof(byte[])] = DbType.Binary,
                [typeof(byte?)] = DbType.Byte,
                [typeof(sbyte?)] = DbType.SByte,
                [typeof(short?)] = DbType.Int16,
                [typeof(ushort?)] = DbType.UInt16,
                [typeof(int?)] = DbType.Int32,
                [typeof(uint?)] = DbType.UInt32,
                [typeof(long?)] = DbType.Int64,
                [typeof(ulong?)] = DbType.UInt64,
                [typeof(float?)] = DbType.Single,
                [typeof(double?)] = DbType.Double,
                [typeof(decimal?)] = DbType.Decimal,
                [typeof(bool?)] = DbType.Boolean,
                [typeof(char?)] = DbType.StringFixedLength,
                [typeof(Guid?)] = DbType.Guid,
                [typeof(DateTime?)] = DbType.DateTime,
                [typeof(DateTimeOffset?)] = DbType.DateTimeOffset,
                [typeof(TimeSpan?)] = DbType.Time,
                [typeof(object)] = DbType.Object
            }.ToImmutableDictionary();

        private static readonly object Sync = new object();
        private static PeregrineConfig current;

        static DefaultConfig()
        {
            current = MakeNewConfig();
        }

        public static PeregrineConfig MakeNewConfig()
        {
            return new PeregrineConfig(
                PeregrineDb.Dialect.SqlServer2012, new DefaultTableNameFactory(), new DefaultColumnNameFactory(), true,
                DefaultSqlTypeMapping);
        }

        /// <summary>
        /// Gets the current config
        /// </summary>
        public static PeregrineConfig Current
        {
            get
            {
                lock (Sync)
                {
                    return current;
                }
            }
        }

        /// <summary>
        /// Gets or sets the default dialect
        /// </summary>
        public static IDialect Dialect
        {
            get => Current.Dialect;
            set => Update(c => c.WithDialect(value));
        }

        /// <summary>
        /// Gets or sets a value indicating whether to verify the affected row count was the expected count after a command.
        /// </summary>
        public static bool VerifyAffectedRowCount
        {
            get => Current.VerifyAffectedRowCount;
            set => Update(c => c.WithVerifyAffectedRowCount(value));
        }

        public static ITableNameFactory TableNameFactory
        {
            get => Current.TableNameFactory;
            set => Update(c => c.WithTableNameFactory(value));
        }

        public static IColumnNameFactory ColumnNameFactory
        {
            get => Current.ColumnNameFactory;
            set => Update(c => c.WithColumnNameFactory(value));
        }

        /// <summary>
        /// Configure the specified type to be mapped to a given db-type
        /// </summary>
        public static void AddSqlTypeMapping(Type type, DbType dbType)
        {
            Update(c => c.AddSqlTypeMapping(type, dbType));
        }

        public static void Update(Func<PeregrineConfig, PeregrineConfig> updater)
        {
            lock (Sync)
            {
                current = updater(current) ?? throw new InvalidOperationException("Updater returned null");
            }
        }

        public static void Reset()
        {
            lock (Sync)
            {
                current = MakeNewConfig();
            }
        }
    }
}