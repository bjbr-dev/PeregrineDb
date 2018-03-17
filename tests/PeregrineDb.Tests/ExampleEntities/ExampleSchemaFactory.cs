// <copyright file="ExampleSchemaFactory.cs" company="Berkeleybross">
// Copyright (c) Berkeleybross. All rights reserved.
// </copyright>
namespace Dapper.MicroCRUD.Tests.ExampleEntities
{
    using Dapper.MicroCRUD.Dialects;
    using Dapper.MicroCRUD.Schema;

    internal static class ExampleSchemaFactory
    {
        public static TableSchema CompositeKeys(this IDialect dialect)
        {
            return dialect.MakeSchema<CompositeKeys>();
        }

        public static TableSchema KeyAlias(this IDialect dialect)
        {
            return dialect.MakeSchema<KeyAlias>();
        }

        public static TableSchema KeyExplicit(this IDialect dialect)
        {
            return dialect.MakeSchema<KeyExplicit>();
        }

        public static TableSchema KeyNotGenerated(this IDialect dialect)
        {
            return dialect.MakeSchema<KeyNotGenerated>();
        }

        public static TableSchema PropertyAlias(this IDialect dialect)
        {
            return dialect.MakeSchema<PropertyAlias>();
        }

        public static TableSchema PropertyAllPossibleTypes(this IDialect dialect)
        {
            return dialect.MakeSchema<PropertyAllPossibleTypes>();
        }

        public static TableSchema PropertyComputed(this IDialect dialect)
        {
            return dialect.MakeSchema<PropertyComputed>();
        }

        public static TableSchema PropertyGenerated(this IDialect dialect)
        {
            return dialect.MakeSchema<PropertyGenerated>();
        }

        public static TableSchema User(this IDialect dialect)
        {
            return dialect.MakeSchema<User>();
        }

        public static TableSchema NoColumns(this IDialect dialect)
        {
            return dialect.MakeSchema<NoColumns>();
        }

        public static TableSchema MakeSchema<TEntity>(this IDialect dialect, ITableNameFactory tableNameFactory = null)
        {
            var factory = new TableSchemaFactory(tableNameFactory ?? new DefaultTableNameFactory(), new DefaultColumnNameFactory());
            return factory.MakeTableSchema(typeof(TEntity), dialect);
        }
    }
}