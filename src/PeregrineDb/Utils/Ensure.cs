namespace PeregrineDb.Utils
{
    using System;

    /// <summary>
    /// Helpers to validate method parameters
    /// </summary>
    internal static class Ensure
    {
        /// <summary>
        /// Throws an <see cref="ArgumentNullException"/> if the value is null
        /// </summary>
        public static void NotNull<T>(T value, string paramName)
            where T : class
        {
            if (value == null)
            {
                throw new ArgumentNullException(paramName);
            }
        }
    }
}