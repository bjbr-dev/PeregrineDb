namespace PeregrineDb.PerformanceTests
{
    using System;
    using System.Data.SqlClient;
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
            Console.WriteLine("Using ConnectionString: " + GetBenchmarks.ConnectionString);
            EnsureDBSetup();
            Console.WriteLine("Database setup complete.");

            BenchmarkSwitcher.FromAssembly(typeof(Program).GetTypeInfo().Assembly).Run(args);
        }

        private static void EnsureDBSetup()
        {
            using (var cnn = new SqlConnection(GetBenchmarks.ConnectionString))
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

        private static void WriteLineColor(string message, ConsoleColor color)
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
