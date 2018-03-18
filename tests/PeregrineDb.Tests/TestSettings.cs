namespace PeregrineDb.Tests
{
    using System;

    public class TestSettings
    {
        internal static string PostgresServerConnectionString { get; } = IsInAppVeyor()
            ? "Server=localhost;Port=5432;User Id=postgres;Password=Password12!; Pooling=false;"
            : "Server=10.10.3.202;Port=5432;User Id=postgres;Password=postgres123; Pooling=false;";

        public static string SqlServerConnectionString { get; } = IsInAppVeyor()
            ? @"Server=(local)\SQL2014;Database=master;User ID=sa;Password=Password12!; Pooling=false"
            : @"Server=localhost; Integrated Security=true; Pooling=false";

        private static bool IsInAppVeyor()
        {
            var result = Environment.GetEnvironmentVariable("APPVEYOR");
            return string.Equals(result, "True", StringComparison.OrdinalIgnoreCase);
        }
    }
}