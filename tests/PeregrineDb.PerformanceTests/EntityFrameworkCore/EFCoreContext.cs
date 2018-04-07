namespace PeregrineDb.PerformanceTests.EntityFrameworkCore
{
    using Microsoft.EntityFrameworkCore;

    public class EFCoreContext : DbContext
    {
        private readonly string _connectionString;

        public EFCoreContext(string connectionString)
        {
            this._connectionString = connectionString;
        }

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder) => optionsBuilder.UseSqlServer(this._connectionString);

        public DbSet<Post> Posts { get; set; }
    }
}