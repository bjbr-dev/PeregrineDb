namespace PeregrineDb.Schema
{
    using System;

    /// <summary>
    /// Represents the identity of a table schema when cached
    /// </summary>
    internal class ConditionsColumnCacheIdentity
        : IEquatable<ConditionsColumnCacheIdentity>
    {
        private readonly Type conditionsType;
        private readonly Type entityType;
        private readonly int hashCode;

        /// <summary>
        /// Initializes a new instance of the <see cref="ConditionsColumnCacheIdentity"/> class.
        /// </summary>
        public ConditionsColumnCacheIdentity(Type conditionsType, Type entityType)
        {
            this.conditionsType = conditionsType;
            this.entityType = entityType;
            unchecked
            {
                this.hashCode = (conditionsType.GetHashCode() * 397) ^ entityType.GetHashCode();
            }
        }

        /// <inheritdoc />
        public bool Equals(ConditionsColumnCacheIdentity other)
        {
            if (ReferenceEquals(null, other))
            {
                return false;
            }

            return this.conditionsType == other.conditionsType &&
                   this.entityType == other.conditionsType;
        }

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj))
            {
                return false;
            }

            if (ReferenceEquals(this, obj))
            {
                return true;
            }

            return obj.GetType() == this.GetType() && this.Equals((ConditionsColumnCacheIdentity)obj);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            return this.hashCode;
        }
    }
}