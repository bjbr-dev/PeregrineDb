using System;

namespace PeregrineDb.Databases.Mapper
{
    using System.Collections.Concurrent;
    using System.Threading;

    internal class QueryCache
    {
        private const int CollectPerItems = 1000;
        private const int CollectHitCountMin = 0;
        private static int collect;
        private static readonly ConcurrentDictionary<Identity, CacheInfo> Items = new ConcurrentDictionary<Identity, CacheInfo>();

        internal static void SetQueryCache(Identity key, CacheInfo value)
        {
            if (Interlocked.Increment(ref collect) == CollectPerItems)
            {
                CollectCacheGarbage();
            }
            Items[key] = value;
        }

        private static void CollectCacheGarbage()
        {
            try
            {
                foreach (var pair in Items)
                {
                    if (pair.Value.GetHitCount() <= CollectHitCountMin)
                    {
                        Items.TryRemove(pair.Key, out var cache);
                    }
                }
            }

            finally
            {
                Interlocked.Exchange(ref collect, 0);
            }
        }

        internal static bool TryGetQueryCache(Identity key, out CacheInfo value)
        {
            if (Items.TryGetValue(key, out value))
            {
                value.RecordHit();
                return true;
            }
            value = null;
            return false;
        }

        public static void Purge()
        {
            Items.Clear();

            TypeDeserializerCache.Purge();
        }

        internal static void PurgeQueryCacheByType(Type type)
        {
            foreach (var entry in Items)
            {
                if (entry.Key.type == type)
                    Items.TryRemove(entry.Key, out var cache);
            }

            TypeDeserializerCache.Purge(type);
        }
    }
}
