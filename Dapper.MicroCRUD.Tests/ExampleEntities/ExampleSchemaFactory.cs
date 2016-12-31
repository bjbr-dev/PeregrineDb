// <copyright file="ExampleSchemaFactory.cs" company="Berkeleybross">
// Copyright (c) Berkeleybross. All rights reserved.
// </copyright>
namespace Dapper.MicroCRUD.Tests.ExampleEntities
{
    using System.Collections.Immutable;
    using Dapper.MicroCRUD.Entities;

    internal static class ExampleSchemaFactory
    {
        public static TableSchema KeyAlias
        {
            get
            {
                var primaryKey = new ColumnSchema("[Key]", "[Id]", "Id", ColumnUsage.PrimaryKey);
                var columns = new[]
                    {
                        primaryKey,
                        new ColumnSchema("[Name]", "[Name]", "Name", ColumnUsage.Column)
                    };

                return new TableSchema("[KeyAlias]", columns.ToImmutableList());
            }
        }

        public static TableSchema KeyNotDefault
        {
            get
            {
                var primaryKey = new ColumnSchema("[Key]", "[Key]", "Key", ColumnUsage.PrimaryKey);
                var columns = new[]
                    {
                        primaryKey,
                        new ColumnSchema("[Name]", "[Name]", "Name", ColumnUsage.Column)
                    };

                return new TableSchema("[KeyNotDefault]", columns.ToImmutableList());
            }
        }

        public static TableSchema KeyNotGenerated
        {
            get
            {
                var primaryKey = new ColumnSchema("[Id]", "[Id]", "Id", ColumnUsage.NotGeneratedPrimaryKey);
                var columns = new[]
                    {
                        primaryKey,
                        new ColumnSchema("[Name]", "[Name]", "Name", ColumnUsage.Column)
                    };

                return new TableSchema("[KeyNotGenerated]", columns.ToImmutableList());
            }
        }

        public static TableSchema PropertyAlias
        {
            get
            {
                var primaryKey = new ColumnSchema("[Id]", "[Id]", "Id", ColumnUsage.PrimaryKey);
                var columns = new[]
                    {
                        primaryKey,
                        new ColumnSchema("[YearsOld]", "[Age]", "Age", ColumnUsage.Column)
                    };

                return new TableSchema("[PropertyAlias]", columns.ToImmutableList());
            }
        }

        public static TableSchema PropertyComputed
        {
            get
            {
                var primaryKey = new ColumnSchema("[Id]", "[Id]", "Id", ColumnUsage.PrimaryKey);
                var columns = new[]
                    {
                        primaryKey,
                        new ColumnSchema("[Name]", "[Name]", "Name", ColumnUsage.Column),
                        new ColumnSchema("[LastUpdated]", "[LastUpdated]", "LastUpdated", ColumnUsage.ComputedColumn)
                    };

                return new TableSchema("[PropertyComputed]", columns.ToImmutableList());
            }
        }

        public static TableSchema PropertyGenerated
        {
            get
            {
                var primaryKey = new ColumnSchema("[Id]", "[Id]", "Id", ColumnUsage.PrimaryKey);
                var columns = new[]
                    {
                        primaryKey,
                        new ColumnSchema("[Name]", "[Name]", "Name", ColumnUsage.Column),
                        new ColumnSchema("[Created]", "[Created]", "Created", ColumnUsage.GeneratedColumn)
                    };

                return new TableSchema("[PropertyGenerated]", columns.ToImmutableList());
            }
        }

        public static TableSchema User
        {
            get
            {
                var primaryKey = new ColumnSchema("[Id]", "[Id]", "Id", ColumnUsage.PrimaryKey);
                var columns = new[]
                    {
                        primaryKey,
                        new ColumnSchema("[Name]", "[Name]", "Name", ColumnUsage.Column),
                        new ColumnSchema("[Age]", "[Age]", "Age", ColumnUsage.Column)
                    };

                return new TableSchema("[Users]", columns.ToImmutableList());
            }
        }
    }
}