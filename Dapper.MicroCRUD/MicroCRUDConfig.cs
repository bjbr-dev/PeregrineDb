// <copyright file="MicroCRUDConfig.cs" company="Berkeleybross">
// Copyright (c) Berkeleybross. All rights reserved.
// </copyright>
namespace Dapper.MicroCRUD
{
    using Dapper.MicroCRUD.Entities;

    /// <summary>
    /// Defines the configuration for MicroCRUD.
    /// </summary>
    public class MicroCRUDConfig
    {
        private static readonly object LockObject = new object();

        private static MicroCRUDConfig @default = new MicroCRUDConfig(
            Dialect.SqlServer2012, new DefaultTableNameResolver(), new DefaultColumnNameResolver());

        /// <summary>
        /// Initializes a new instance of the <see cref="MicroCRUDConfig"/> class.
        /// </summary>
        public MicroCRUDConfig(
            Dialect dialect,
            ITableNameResolver tableNameResolver,
            IColumnNameResolver columnNameResolver)
        {
            this.Dialect = dialect;
            this.TableNameResolver = tableNameResolver;
            this.ColumnNameResolver = columnNameResolver;
        }

        /// <summary>
        /// Gets or sets the default dialect
        /// </summary>
        public static Dialect DefaultDialect
        {
            get { return Default.Dialect; }
            set { Default = Default.WithDialect(value); }
        }

        /// <summary>
        /// Gets or sets the resolver to use when getting the table name of an entity.
        /// </summary>
        public static ITableNameResolver DefaultTableNameResolver
        {
            get { return Default.TableNameResolver; }
            set { Default = Default.WithTableNameResolver(value); }
        }

        /// <summary>
        /// Gets or sets the resolver to use when getting the column name of an entity field.
        /// </summary>
        public static IColumnNameResolver DefaultColumnNameResolver
        {
            get { return Default.ColumnNameResolver; }
            set { Default = Default.WithColumnNameResolver(value); }
        }

        /// <summary>
        /// Gets the dialect
        /// </summary>
        internal Dialect Dialect { get; }

        /// <summary>
        /// Gets the table name resolver
        /// </summary>
        internal ITableNameResolver TableNameResolver { get; }

        /// <summary>
        /// Gets the column name resolvers
        /// </summary>
        internal IColumnNameResolver ColumnNameResolver { get; }

        /// <summary>
        /// Gets or sets the default configuration object
        /// </summary>
        private static MicroCRUDConfig Default
        {
            get
            {
                lock (LockObject)
                {
                    return @default;
                }
            }

            set
            {
                lock (LockObject)
                {
                    @default = value;
                }
            }
        }

        /// <summary>
        /// Gets the <see cref="TableSchemaFactory"/> for the specified dialect (Null means use default)
        /// </summary>
        internal static MicroCRUDConfig GetConfig(Dialect dialect)
        {
            if (dialect == null)
            {
                return Default;
            }

            return Default.WithDialect(dialect);
        }

        /// <summary>
        /// Makes a new <see cref="MicroCRUDConfig"/> with the specified <paramref name="newDialect"/>.
        /// </summary>
        private MicroCRUDConfig WithDialect(Dialect newDialect)
        {
            if (newDialect == null || newDialect.Name == this.Dialect.Name)
            {
                return this;
            }

            return new MicroCRUDConfig(newDialect, this.TableNameResolver, this.ColumnNameResolver);
        }

        /// <summary>
        /// Makes a new <see cref="MicroCRUDConfig"/> with the specified <paramref name="newTableNameResolver"/>.
        /// </summary>
        private MicroCRUDConfig WithTableNameResolver(ITableNameResolver newTableNameResolver)
        {
            return new MicroCRUDConfig(this.Dialect, newTableNameResolver, this.ColumnNameResolver);
        }

        /// <summary>
        /// Makes a new <see cref="MicroCRUDConfig"/> with the specified <paramref name="newColumnNameResolver"/>.
        /// </summary>
        private MicroCRUDConfig WithColumnNameResolver(IColumnNameResolver newColumnNameResolver)
        {
            return new MicroCRUDConfig(this.Dialect, this.TableNameResolver, newColumnNameResolver);
        }
    }
}