// <copyright file="EFContext.cs" company="Berkeleybross">
// Copyright (c) Berkeleybross. All rights reserved.
// </copyright>

namespace PeregrineDb.PerformanceTests.EntityFramework
{
    using System.Data.Common;
    using System.Data.Entity;

    public class EFContext : DbContext
    {
        public EFContext(DbConnection connection, bool owned = false)
            : base(connection, owned)
        {
        }

        public DbSet<Post> Posts { get; set; }
    }
}
