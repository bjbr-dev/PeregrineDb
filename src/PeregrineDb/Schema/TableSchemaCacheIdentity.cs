// <copyright file="TableSchemaCacheIdentity.cs" company="Berkeleybross">
// Copyright (c) Berkeleybross. All rights reserved.
// </copyright>

namespace PeregrineDb.Schema
{
    using System;

    /// <summary>
    /// Represents the identity of a table schema when cached.
    /// </summary>
    internal class TableSchemaCacheIdentity
        : IEquatable<TableSchemaCacheIdentity>
    {
        private readonly Type type;
        private readonly int hashCode;

        /// <summary>
        /// Initializes a new instance of the <see cref="TableSchemaCacheIdentity"/> class.
        /// </summary>
        public TableSchemaCacheIdentity(Type type)
        {
            this.type = type;
            this.hashCode = this.type.GetHashCode();
        }

        /// <inheritdoc />
        public bool Equals(TableSchemaCacheIdentity other)
        {
            if (ReferenceEquals(null, other))
            {
                return false;
            }

            return this.type == other.type;
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

            return obj.GetType() == this.GetType() && this.Equals((TableSchemaCacheIdentity)obj);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            return this.hashCode;
        }
    }
}