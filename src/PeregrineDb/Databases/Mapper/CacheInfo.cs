namespace PeregrineDb.Databases.Mapper
{
    using System;
    using System.Data;
    using System.Threading;

    internal class CacheInfo
    {
        private int hitCount;

        public DeserializerState Deserializer { get; set; }

        public Action<IDbCommand, object> ParamReader { get; set; }


        public int GetHitCount()
        {
            return Interlocked.CompareExchange(ref this.hitCount, 0, 0);
        }

        public void RecordHit()
        {
            Interlocked.Increment(ref this.hitCount);
        }
    }
}