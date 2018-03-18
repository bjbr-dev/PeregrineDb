namespace PeregrineDb.Tests.Utils.Pooling
{
    using System;

    public class PooledInstance<T>
        : IDisposable
    {
        private readonly Action<PooledInstance<T>> onDispose;

        public PooledInstance(T item, Action<PooledInstance<T>> onDispose)
        {
            this.Item = item;
            this.onDispose = onDispose;
        }

        public T Item { get; }

        public void Dispose()
        {
            this.onDispose(this);
        }
    }
}