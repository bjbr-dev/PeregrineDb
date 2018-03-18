namespace PeregrineDb.Tests.Utils
{
    using System;
    using System.Diagnostics;

    public class ProcessHelpers
    {
        private static readonly Lazy<int> LazyCurrentProcessId = new Lazy<int>(
            () =>
            {
                using (var process = Process.GetCurrentProcess())
                {
                    return process.Id;
                }
            });

        public static int CurrentProcessId => LazyCurrentProcessId.Value;

        public static bool IsRunning(int id)
        {
            try
            {
                using (Process.GetProcessById(id))
                {
                }

                return true;
            }
            catch (Exception ex) when (ex is InvalidOperationException || ex is ArgumentException)
            {
                return false;
            }
        }
    }
}