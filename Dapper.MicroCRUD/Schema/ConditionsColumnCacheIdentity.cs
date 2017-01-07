// <copyright file="ConditionsColumnCacheIdentity.cs" company="Berkeleybross">
// Copyright (c) Berkeleybross. All rights reserved.
// </copyright>
namespace Dapper.MicroCRUD.Schema
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
        private readonly string dialectName;
        private readonly int hashCode;

        /// <summary>
        /// Initializes a new instance of the <see cref="ConditionsColumnCacheIdentity"/> class.
        /// </summary>
        public ConditionsColumnCacheIdentity(Type conditionsType, Type entityType, string dialectName)
        {
            this.conditionsType = conditionsType;
            this.entityType = entityType;
            this.dialectName = dialectName;
            unchecked
            {
                this.hashCode = (conditionsType.GetHashCode() * 397) ^
                                (entityType.GetHashCode() * 397) ^
                                dialectName.GetHashCode();
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
                   this.entityType == other.conditionsType &&
                   string.Equals(this.dialectName, other.dialectName);
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