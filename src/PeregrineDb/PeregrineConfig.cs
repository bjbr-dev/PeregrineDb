namespace PeregrineDb
{
    using System;
    using System.Collections.Generic;
    using System.Collections.Immutable;
    using System.Data;
    using System.Linq;
    using PeregrineDb.Dialects;
    using PeregrineDb.Dialects.Postgres;
    using PeregrineDb.Schema;

    /// <summary>
    /// Defines the configuration for Peregrine.
    /// </summary>
    public class PeregrineConfig
    {
        internal static readonly ImmutableDictionary<Type, DbType> DefaultSqlTypeMapping = new Dictionary<Type, DbType>
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

        public static PeregrineConfig SqlServer2012 => new PeregrineConfig(
            PeregrineDb.Dialect.SqlServer2012, new AtttributeTableNameFactory(), new AttributeColumnNameFactory(), true, DefaultSqlTypeMapping);

        public static PeregrineConfig Postgres => new PeregrineConfig(
            PeregrineDb.Dialect.PostgreSql, new PostgresAttributeTableNameFactory(), new PostgresAttributeColumnNameFactory(), true, DefaultSqlTypeMapping);

        private readonly TableSchemaFactory tableSchemaFactory;

        /// <summary>
        /// Initializes a new instance of the <see cref="PeregrineConfig"/> class.
        /// </summary>
        public PeregrineConfig(
            IDialect dialect,
            ITableNameFactory tableNameFactory,
            IColumnNameFactory columnNameFactory,
            bool verifyAffectedRowCount,
            ImmutableDictionary<Type, DbType> sqlTypeMappings)
        {
            this.Dialect = dialect ?? throw new ArgumentNullException(nameof(dialect));
            this.TableNameFactory = tableNameFactory ?? throw new ArgumentNullException(nameof(tableNameFactory));
            this.ColumnNameFactory = columnNameFactory ?? throw new ArgumentNullException(nameof(columnNameFactory));
            this.VerifyAffectedRowCount = verifyAffectedRowCount;
            this.SqlTypeMappings = sqlTypeMappings ?? throw new ArgumentNullException(nameof(sqlTypeMappings));

            var fastMappings = sqlTypeMappings.ToDictionary(k => k.Key, v => v.Value);
            this.tableSchemaFactory = new TableSchemaFactory(dialect, tableNameFactory, columnNameFactory, fastMappings);
        }

        /// <summary>
        /// Gets the dialect
        /// </summary>
        public IDialect Dialect { get; }

        public ITableNameFactory TableNameFactory { get; }

        public IColumnNameFactory ColumnNameFactory { get; }

        public ImmutableDictionary<Type, DbType> SqlTypeMappings { get; }

        public bool VerifyAffectedRowCount { get; }

        /// <summary>
        /// Gets a value indicating whether the affected row count should be verified or not.
        /// </summary>
        public bool ShouldVerifyAffectedRowCount(bool? @override)
        {
            return @override ?? this.VerifyAffectedRowCount;
        }

        /// <summary>
        /// Creates a new <see cref="PeregrineConfig"/> with the specified <paramref name="dialect"/>.
        /// </summary>
        public PeregrineConfig WithDialect(IDialect dialect)
        {
            return new PeregrineConfig(dialect, this.TableNameFactory, this.ColumnNameFactory, this.VerifyAffectedRowCount, this.SqlTypeMappings);
        }

        public PeregrineConfig WithTableNameFactory(ITableNameFactory factory)
        {
            return new PeregrineConfig(this.Dialect, factory, this.ColumnNameFactory, this.VerifyAffectedRowCount, this.SqlTypeMappings);
        }

        public PeregrineConfig WithColumnNameFactory(IColumnNameFactory factory)
        {
            return new PeregrineConfig(this.Dialect, this.TableNameFactory, factory, this.VerifyAffectedRowCount, this.SqlTypeMappings);
        }

        public PeregrineConfig WithVerifyAffectedRowCount(bool verify)
        {
            return new PeregrineConfig(this.Dialect, this.TableNameFactory, this.ColumnNameFactory, verify, this.SqlTypeMappings);
        }

        public PeregrineConfig AddSqlTypeMapping(Type type, DbType dbType)
        {
            var mappings = this.SqlTypeMappings.SetItem(type, dbType);
            return new PeregrineConfig(this.Dialect, this.TableNameFactory, this.ColumnNameFactory, this.VerifyAffectedRowCount, mappings);
        }

        public TableSchema GetTableSchema(Type entityType)
        {
            return this.tableSchemaFactory.GetTableSchema(entityType);
        }

        public ImmutableArray<ConditionColumnSchema> GetConditionsSchema(
            Type entityType,
            TableSchema tableSchema,
            Type conditionsType)
        {
            return this.tableSchemaFactory.GetConditionsSchema(entityType, tableSchema, conditionsType);
        }
    }
}