// <copyright file="PeregrineConfig.cs" company="Berkeleybross">
// Copyright (c) Berkeleybross. All rights reserved.
// </copyright>

namespace PeregrineDb
{
    using System;
    using System.Collections.Generic;
    using System.Collections.Immutable;
    using System.Data;
    using PeregrineDb.Dialects;
    using PeregrineDb.Dialects.Postgres;
    using PeregrineDb.Dialects.SqlServer2012;
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

        private readonly ISqlNameEscaper sqlNameEscaper;

        /// <summary>
        /// Initializes a new instance of the <see cref="PeregrineConfig"/> class.
        /// </summary>
        public PeregrineConfig(
            IDialect dialect,
            ISqlNameEscaper sqlNameEscaper,
            ITableNameConvention tableNameConvention,
            IColumnNameConvention columnNameConvention,
            bool verifyAffectedRowCount,
            ImmutableDictionary<Type, DbType> sqlTypeMappings)
        {
            this.Dialect = dialect ?? throw new ArgumentNullException(nameof(dialect));
            this.TableNameConvention = tableNameConvention ?? throw new ArgumentNullException(nameof(tableNameConvention));
            this.ColumnNameConvention = columnNameConvention ?? throw new ArgumentNullException(nameof(columnNameConvention));
            this.VerifyAffectedRowCount = verifyAffectedRowCount;
            this.SqlTypeMappings = sqlTypeMappings ?? throw new ArgumentNullException(nameof(sqlTypeMappings));
            this.sqlNameEscaper = sqlNameEscaper ?? throw new ArgumentNullException(nameof(sqlNameEscaper));
        }

        public static PeregrineConfig SqlServer2012
        {
            get
            {
                var nameEscaper = new SqlServer2012NameEscaper();
                return new PeregrineConfig(
                    PeregrineDb.Dialect.SqlServer2012,
                    nameEscaper,
                    new AtttributeTableNameConvention(nameEscaper),
                    new AttributeColumnNameConvention(nameEscaper),
                    true,
                    DefaultSqlTypeMapping);
            }
        }

        public static PeregrineConfig Postgres
        {
            get
            {
                var nameEscaper = new PostgresNameEscaper();
                return new PeregrineConfig(
                    PeregrineDb.Dialect.PostgreSql,
                    nameEscaper,
                    new PostgresAttributeTableNameConvention(nameEscaper),
                    new PostgresAttributeColumnNameConvention(nameEscaper),
                    true,
                    DefaultSqlTypeMapping);
            }
        }

        /// <summary>
        /// Gets the dialect.
        /// </summary>
        public IDialect Dialect { get; }

        public ITableNameConvention TableNameConvention { get; }

        public IColumnNameConvention ColumnNameConvention { get; }

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
            return new PeregrineConfig(dialect, this.sqlNameEscaper, this.TableNameConvention, this.ColumnNameConvention, this.VerifyAffectedRowCount, this.SqlTypeMappings);
        }

        public PeregrineConfig WithTableNameConvention(ITableNameConvention convention)
        {
            return new PeregrineConfig(this.Dialect, this.sqlNameEscaper, convention, this.ColumnNameConvention, this.VerifyAffectedRowCount, this.SqlTypeMappings);
        }

        public PeregrineConfig WithColumnNameConvention(IColumnNameConvention convention)
        {
            return new PeregrineConfig(this.Dialect, this.sqlNameEscaper, this.TableNameConvention, convention, this.VerifyAffectedRowCount, this.SqlTypeMappings);
        }

        public PeregrineConfig WithVerifyAffectedRowCount(bool verify)
        {
            return new PeregrineConfig(this.Dialect, this.sqlNameEscaper, this.TableNameConvention, this.ColumnNameConvention, verify, this.SqlTypeMappings);
        }

        public PeregrineConfig AddSqlTypeMapping(Type type, DbType dbType)
        {
            var mappings = this.SqlTypeMappings.SetItem(type, dbType);
            return new PeregrineConfig(this.Dialect, this.sqlNameEscaper, this.TableNameConvention, this.ColumnNameConvention, this.VerifyAffectedRowCount, mappings);
        }
    }
}