namespace PeregrineDb.Tests.Utils.Pooling
{
    using System;
    using System.Collections.Generic;
    using PeregrineDb.Utils;

    public class ObjectPool<T>
        where T : class
    {
        private readonly object sync;
        private readonly Stack<PooledInstance<T>> instances;
        private readonly Func<T> factory;

        public ObjectPool(Func<T> factory)
        {
            Ensure.NotNull(factory, nameof(factory));

            this.sync = new object();
            this.instances = new Stack<PooledInstance<T>>();
            this.factory = factory;
        }

        public PooledInstance<T> Acquire()
        {
            lock (this.sync)
            {
                if (this.instances.Count > 0)
                {
                    return this.instances.Pop();
                }
            }

            var newItem = this.factory();
            if (newItem == null)
            {
                throw new InvalidOperationException("Factory returned a null object");
            }

            return new PooledInstance<T>(newItem, this.DisposeInstance);
        }

        private void DisposeInstance(PooledInstance<T> instance)
        {
            lock (this.sync)
            {
                this.instances.Push(instance);
            }
        }
    }
}