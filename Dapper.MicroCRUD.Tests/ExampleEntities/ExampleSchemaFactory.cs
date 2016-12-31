// <copyright file="ExampleSchemaFactory.cs" company="Berkeleybross">
// Copyright (c) Berkeleybross. All rights reserved.
// </copyright>
namespace Dapper.MicroCRUD.Tests.ExampleEntities
{
    using System.Collections.Immutable;
    using Dapper.MicroCRUD.Entities;

    internal static class ExampleSchemaFactory
    {
        public static TableSchema User
        {
            get
            {
                var primaryKey = new ColumnSchema("[Id]", "[Id]", "Id", true, true);
                var columns = new[]
                    {
                        primaryKey,
                        new ColumnSchema("[Name]", "[Name]", "Name", false, false),
                        new ColumnSchema("[Age]", "[Age]", "Age", false, false)
                    };

                return new TableSchema("[Users]", columns.ToImmutableList());
            }
        }

        public static TableSchema KeyNotDefault
        {
            get
            {
                var primaryKey = new ColumnSchema("[Key]", "[Key]", "Key", true, true);
                var columns = new[]
                    {
                        primaryKey,
                        new ColumnSchema("[Name]", "[Name]", "Name", false, false)
                    };

                return new TableSchema("[KeyNotDefault]", columns.ToImmutableList());
            }
        }

        public static TableSchema KeyAlias
        {
            get
            {
                var primaryKey = new ColumnSchema("[Key]", "[Id]", "Id", true, true);
                var columns = new[]
                    {
                        primaryKey,
                        new ColumnSchema("[Name]", "[Name]", "Name", false, false)
                    };

                return new TableSchema("[KeyAlias]", columns.ToImmutableList());
            }
        }

        public static TableSchema PropertyAlias
        {
            get
            {
                var primaryKey = new ColumnSchema("[Id]", "[Id]", "Id", true, true);
                var columns = new[]
                    {
                        primaryKey,
                        new ColumnSchema("[YearsOld]", "[Age]", "Age", false, false)
                    };

                return new TableSchema("[PropertyAlias]", columns.ToImmutableList());
            }
        }

        public static TableSchema KeyNotGenerated
        {
            get
            {
                var primaryKey = new ColumnSchema("[Id]", "[Id]", "Id", true, false);
                var columns = new[]
                    {
                        primaryKey,
                        new ColumnSchema("[Name]", "[Name]", "Name", false, false)
                    };

                return new TableSchema("[KeyNotGenerated]", columns.ToImmutableList());
            }
        }
    }
}