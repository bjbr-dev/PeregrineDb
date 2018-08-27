// <copyright file="EFCoreContext.cs" company="Berkeleybross">
// Copyright (c) Berkeleybross. All rights reserved.
// </copyright>

namespace PeregrineDb.PerformanceTests.EntityFrameworkCore
{
    using Microsoft.EntityFrameworkCore;

    public class EFCoreContext : DbContext
    {
        private readonly string connectionString;

        public EFCoreContext(string connectionString)
        {
            this.connectionString = connectionString;
        }

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder) => optionsBuilder.UseSqlServer(this.connectionString);

        public DbSet<Post> Posts { get; set; }
    }
}