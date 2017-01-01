// <copyright file="TableSchemaCache.cs" company="Berkeleybross">
// Copyright (c) Berkeleybross. All rights reserved.
// </copyright>
namespace Dapper.MicroCRUD.Entities
{
    using System;
    using System.Collections.Concurrent;

    /// <summary>
    /// Caches table schemas
    /// </summary>
    internal static class TableSchemaCache
    {
        private static readonly ConcurrentDictionary<TableSchemaIdentity, TableSchema> Schemas =
            new ConcurrentDictionary<TableSchemaIdentity, TableSchema>();

        /// <summary>
        /// Gets the <see cref="TableSchema"/> for the specified entityType and dialect.
        /// </summary>
        internal static TableSchema GetTableSchema(Dialect dialect, Type entityType)
        {
            return GetTableSchema(entityType, MicroCRUDConfig.GetConfig(dialect));
        }

        /// <summary>
        /// Gets the <see cref="TableSchema"/> for the specified entityType and dialect.
        /// </summary>
        internal static TableSchema GetTableSchema(Type entityType, MicroCRUDConfig config)
        {
            var key = new TableSchemaIdentity(entityType, config.Dialect.Name);

            TableSchema result;
            if (Schemas.TryGetValue(key, out result))
            {
                return result;
            }

            var schema = TableSchemaFactory.GetTableSchema(entityType, config);
            Schemas[key] = schema;
            return schema;
        }

        private class TableSchemaIdentity
            : IEquatable<TableSchemaIdentity>
        {
            private readonly Type type;
            private readonly string dialectName;
            private readonly int hashCode;

            public TableSchemaIdentity(Type type, string dialectName)
            {
                this.type = type;
                this.dialectName = dialectName;
                unchecked
                {
                    this.hashCode = (this.type.GetHashCode() * 397) ^ this.dialectName.GetHashCode();
                }
            }

            public bool Equals(TableSchemaIdentity other)
            {
                if (ReferenceEquals(null, other))
                {
                    return false;
                }

                if (ReferenceEquals(this, other))
                {
                    return true;
                }

                return this.type == other.type && string.Equals(this.dialectName, other.dialectName);
            }

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

                return obj.GetType() == this.GetType() && this.Equals((TableSchemaIdentity)obj);
            }

            public override int GetHashCode()
            {
                return this.hashCode;
            }
        }
    }
}