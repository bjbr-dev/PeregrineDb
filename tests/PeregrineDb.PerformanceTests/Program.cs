namespace PeregrineDb.PerformanceTests
{
    using System;
    using System.Collections.Generic;
    using System.Data.SqlClient;
    using System.Linq;
    using System.Reflection;
    using BenchmarkDotNet.Running;

    public static class Program
    {
        public static void Main(string[] args)
        {
#if DEBUG
            WriteLineColor("Warning: DEBUG configuration; performance may be impacted!", ConsoleColor.Red);
            Console.WriteLine();
#endif
            Console.WriteLine("Welcome to Dapper's ORM performance benchmark suite, based on BenchmarkDotNet.");
            Console.Write("  If you find a problem, please report it at: ");
            WriteLineColor("https://github.com/StackExchange/Dapper", ConsoleColor.Blue);
            Console.WriteLine("  Or if you're up to it, please submit a pull request! We welcome new additions.");
            Console.WriteLine();

            if (args.Length == 0)
            {
                Console.WriteLine("Optional arguments:");
                WriteColor("  --all", ConsoleColor.Blue);
                Console.WriteLine(": run all benchmarks");
                Console.WriteLine();
            }
            Console.WriteLine("Using ConnectionString: " + BenchmarkBase.ConnectionString);
            EnsureDBSetup();
            Console.WriteLine("Database setup complete.");

            if (args.Any(a => a == "--all") || true)
            {
                Console.WriteLine("Iterations: " + BenchmarkBase.Iterations);
                var benchmarks = new List<Benchmark>();
                var benchTypes = Assembly.GetEntryAssembly().DefinedTypes.Where(t => t.IsSubclassOf(typeof(BenchmarkBase)));
                WriteLineColor("Running full benchmarks suite", ConsoleColor.Green);
                foreach (var b in benchTypes)
                {
                    benchmarks.AddRange(BenchmarkConverter.TypeToBenchmarks(b));
                }
                BenchmarkRunner.Run(benchmarks.ToArray(), null);
            }
            else
            {
                Console.WriteLine("Iterations: " + BenchmarkBase.Iterations);
                BenchmarkSwitcher.FromAssembly(typeof(Program).GetTypeInfo().Assembly).Run(args);
            }
        }

        private static void EnsureDBSetup()
        {
            using (var cnn = new SqlConnection(BenchmarkBase.ConnectionString))
            {
                cnn.Open();
                var cmd = cnn.CreateCommand();
                cmd.CommandText = @"
If (Object_Id('Posts') Is Null)
Begin
	Create Table Posts
	(
		Id int identity primary key, 
		[Text] varchar(max) not null, 
		CreationDate datetime not null, 
		LastChangeDate datetime not null,
		Counter1 int,
		Counter2 int,
		Counter3 int,
		Counter4 int,
		Counter5 int,
		Counter6 int,
		Counter7 int,
		Counter8 int,
		Counter9 int
	);
	   
	Set NoCount On;
	Declare @i int = 0;

	While @i <= 5001
	Begin
		Insert Posts ([Text],CreationDate, LastChangeDate) values (replicate('x', 2000), GETDATE(), GETDATE());
		Set @i = @i + 1;
	End
End
";
                cmd.Connection = cnn;
                cmd.ExecuteNonQuery();
            }
        }

        public static void WriteLineColor(string message, ConsoleColor color)
        {
            var orig = Console.ForegroundColor;
            Console.ForegroundColor = color;
            Console.WriteLine(message);
            Console.ForegroundColor = orig;
        }

        public static void WriteColor(string message, ConsoleColor color)
        {
            var orig = Console.ForegroundColor;
            Console.ForegroundColor = color;
            Console.Write(message);
            Console.ForegroundColor = orig;
        }
    }
}
