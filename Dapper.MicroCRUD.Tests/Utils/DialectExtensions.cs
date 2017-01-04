// <copyright file="DialectExtensions.cs" company="Berkeleybross">
// Copyright (c) Berkeleybross. All rights reserved.
// </copyright>
namespace Dapper.MicroCRUD.Tests.Utils
{
    using System.Collections.Generic;
    using System.Collections.Immutable;
    using Dapper.MicroCRUD.Schema;

    internal static class DialectExtensions
    {
        public static TableSchema MakeTableSchema(this Dialect dialect, string name, IEnumerable<ColumnSchema> columns)
        {
            return new TableSchema(dialect.EscapeMostReservedCharacters(name), columns.ToImmutableArray());
        }

        public static ColumnSchema MakeColumnSchema(this Dialect dialect, string name, ColumnUsage usage)
        {
            return dialect.MakeColumnSchema(name, name, usage);
        }

        public static ColumnSchema MakeColumnSchema(
            this Dialect dialect,
            string propertyName,
            string columnName,
            ColumnUsage usage)
        {
            return new ColumnSchema(
                dialect.EscapeMostReservedCharacters(columnName),
                dialect.EscapeMostReservedCharacters(propertyName),
                propertyName,
                usage);
        }
    }
}