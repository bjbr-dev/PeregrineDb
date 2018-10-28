// <copyright file="Dialect.cs" company="Berkeleybross">
// Copyright (c) Berkeleybross. All rights reserved.
// </copyright>

namespace PeregrineDb
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using PeregrineDb.Dialects;
    using PeregrineDb.Dialects.Postgres;
    using PeregrineDb.Dialects.SqlServer2012;
    using PeregrineDb.Schema;

    /// <summary>
    /// Defines the SQL to generate when targeting specific vendor implementations.
    /// </summary>
    public static class Dialect
    {
        internal static Dictionary<Type, DbType> TypeMapping { get; set; } = new Dictionary<Type, DbType>
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
            };

        /// <summary>
        /// Gets the Dialect for Microsoft SQL Server 2012.
        /// </summary>
        public static IDialect SqlServer2012 { get; } = new SqlServer2012Dialect(
            new TableSchemaFactory(
                new SqlServer2012NameEscaper(),
                new AtttributeTableNameConvention(new SqlServer2012NameEscaper()),
                new AttributeColumnNameConvention(new SqlServer2012NameEscaper()),
                TypeMapping));

        /// <summary>
        /// Gets the dialect for PostgreSQL.
        /// </summary>
        public static IDialect PostgreSql { get; } = new PostgreSqlDialect(
            new TableSchemaFactory(
                new PostgresNameEscaper(),
                new PostgresAttributeTableNameConvention(new PostgresNameEscaper()),
                new PostgresAttributeColumnNameConvention(new PostgresNameEscaper()),
                TypeMapping));
    }
}