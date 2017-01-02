// <copyright file="ExampleSchemaFactory.cs" company="Berkeleybross">
// Copyright (c) Berkeleybross. All rights reserved.
// </copyright>
namespace Dapper.MicroCRUD.Tests.ExampleEntities
{
    using Dapper.MicroCRUD.Entities;

    internal static class ExampleSchemaFactory
    {
        public static TableSchema CompositeKeys(this Dialect dialect)
        {
            var columns = new[]
                {
                    dialect.MakeColumnSchema("Key1", ColumnUsage.NotGeneratedPrimaryKey),
                    dialect.MakeColumnSchema("Key2", ColumnUsage.NotGeneratedPrimaryKey),
                    dialect.MakeColumnSchema("Name", ColumnUsage.Column)
                };

            return dialect.MakeTableSchema("CompositeKeys", columns);
        }

        public static TableSchema KeyAlias(this Dialect dialect)
        {
            var primaryKey = dialect.MakeColumnSchema("Id", "Key", ColumnUsage.ComputedPrimaryKey);
            var columns = new[]
                {
                    primaryKey,
                    dialect.MakeColumnSchema("Name", ColumnUsage.Column)
                };

            return dialect.MakeTableSchema("KeyAlias", columns);
        }

        public static TableSchema KeyNotDefault(this Dialect dialect)
        {
            var primaryKey = dialect.MakeColumnSchema("Key", ColumnUsage.ComputedPrimaryKey);
            var columns = new[]
                {
                    primaryKey,
                    dialect.MakeColumnSchema("Name", ColumnUsage.Column)
                };

            return dialect.MakeTableSchema("KeyNotDefault", columns);
        }

        public static TableSchema KeyNotGenerated(this Dialect dialect)
        {
            var primaryKey = dialect.MakeColumnSchema("Id", ColumnUsage.NotGeneratedPrimaryKey);
            var columns = new[]
                {
                    primaryKey,
                    dialect.MakeColumnSchema("Name", ColumnUsage.Column)
                };

            return dialect.MakeTableSchema("KeyNotGenerated", columns);
        }

        public static TableSchema PropertyAlias(this Dialect dialect)
        {
            var primaryKey = dialect.MakeColumnSchema("Id", ColumnUsage.ComputedPrimaryKey);
            var columns = new[]
                {
                    primaryKey,
                    dialect.MakeColumnSchema("Age", "YearsOld", ColumnUsage.Column)
                };

            return dialect.MakeTableSchema("PropertyAlias", columns);
        }

        public static TableSchema PropertyComputed(this Dialect dialect)
        {
            var primaryKey = dialect.MakeColumnSchema("Id", ColumnUsage.ComputedPrimaryKey);
            var columns = new[]
                {
                    primaryKey,
                    dialect.MakeColumnSchema("Name", ColumnUsage.Column),
                    dialect.MakeColumnSchema("LastUpdated", ColumnUsage.ComputedColumn)
                };

            return dialect.MakeTableSchema("PropertyComputed", columns);
        }

        public static TableSchema PropertyGenerated(this Dialect dialect)
        {
            var primaryKey = dialect.MakeColumnSchema("Id", ColumnUsage.ComputedPrimaryKey);
            var columns = new[]
                {
                    primaryKey,
                    dialect.MakeColumnSchema("Name", ColumnUsage.Column),
                    dialect.MakeColumnSchema("Created", ColumnUsage.GeneratedColumn)
                };

            return dialect.MakeTableSchema("PropertyGenerated", columns);
        }

        public static TableSchema User(this Dialect dialect)
        {
            var primaryKey = dialect.MakeColumnSchema("Id", ColumnUsage.ComputedPrimaryKey);
            var columns = new[]
                {
                    primaryKey,
                    dialect.MakeColumnSchema("Name", ColumnUsage.Column),
                    dialect.MakeColumnSchema("Age", ColumnUsage.Column)
                };

            return dialect.MakeTableSchema("Users", columns);
        }
    }
}