namespace PeregrineDb.Databases.Mapper
{
    using System;
    using System.Text;

    internal static class StringBuilderPool
    {
        [ThreadStatic]
        private static StringBuilder perThreadCache;

        public static StringBuilder Acquire()
        {
            var tmp = perThreadCache;
            if (tmp != null)
            {
                perThreadCache = null;
                tmp.Length = 0;
                return tmp;
            }

            return new StringBuilder();
        }

        public static string __ToStringRecycle(this StringBuilder obj)
        {
            if (obj == null)
            {
                return "";
            }

            var s = obj.ToString();
            perThreadCache = perThreadCache ?? obj;
            return s;
        }
    }
}