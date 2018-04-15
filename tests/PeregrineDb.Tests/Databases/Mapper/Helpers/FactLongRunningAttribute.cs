namespace PeregrineDb.Tests.Databases.Mapper.Helpers
{
    using System;
    using Xunit;

    [AttributeUsage(AttributeTargets.Method, AllowMultiple = false)]
    public sealed class FactLongRunningAttribute : FactAttribute
    {
        public FactLongRunningAttribute()
        {
#if !LONG_RUNNING
            this.Skip = "Long running";
#endif
        }
    }
}