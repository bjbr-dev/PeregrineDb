// <copyright file="MicroCRUDConfig.cs" company="Berkeleybross">
// Copyright (c) Berkeleybross. All rights reserved.
// </copyright>
namespace Dapper.MicroCRUD
{
    using System;
    using Dapper.MicroCRUD.Dialects;
    using Dapper.MicroCRUD.Schema;
    using Dapper.MicroCRUD.Utils;

    /// <summary>
    /// Defines the configuration for MicroCRUD.
    /// </summary>
    public class MicroCRUDConfig
    {
        private static readonly object LockObject = new object();
        private static MicroCRUDConfig current;

        static MicroCRUDConfig()
        {
            var defaultSchemaFactory = new TableSchemaFactory(new DefaultTableNameFactory(), new DefaultColumnNameFactory());
            current = new MicroCRUDConfig(MicroCRUD.Dialect.PostgreSql, defaultSchemaFactory, true);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="MicroCRUDConfig"/> class.
        /// </summary>
        internal MicroCRUDConfig(IDialect dialect, TableSchemaFactory schemaFactory, bool verifyAffectedRowCount)
        {
            Ensure.NotNull(dialect, nameof(dialect));
            Ensure.NotNull(schemaFactory, nameof(schemaFactory));

            this.Dialect = dialect;
            this.SchemaFactory = schemaFactory;
            this.VerifyAffectedRowCount = verifyAffectedRowCount;
        }

        /// <summary>
        /// Gets or sets the default dialect
        /// </summary>
        public static IDialect DefaultDialect
        {
            get { return Current.Dialect; }
            set { SetCurrent(c => c.WithDialect(value)); }
        }

        /// <summary>
        /// Gets or sets a value indicating whether to verify the affected row count was the expected count after a command.
        /// </summary>
        public static bool DefaultVerifyAffectedRowCount
        {
            get { return Current.VerifyAffectedRowCount; }
            set { SetCurrent(c => new MicroCRUDConfig(c.Dialect, c.SchemaFactory, value)); }
        }

        /// <summary>
        /// Gets the current config
        /// </summary>
        internal static MicroCRUDConfig Current
        {
            get
            {
                lock (LockObject)
                {
                    return current;
                }
            }
        }

        /// <summary>
        /// Gets the dialect
        /// </summary>
        internal IDialect Dialect { get; }

        /// <summary>
        /// Gets the table schema factory
        /// </summary>
        internal TableSchemaFactory SchemaFactory { get; }

        private bool VerifyAffectedRowCount { get; }

        /// <summary>
        /// Sets the method used to get the column name from a property.
        /// </summary>
        public static void SetTableNameFactory(ITableNameFactory factory)
        {
            Ensure.NotNull(factory, nameof(factory));

            SetCurrent(c => c.WithSchemaFactory(c.SchemaFactory.WithTableNameFactory(factory)));
        }

        /// <summary>
        /// Sets the method used to get the column name from a property.
        /// </summary>
        public static void SetColumnNameFactory(IColumnNameFactory factory)
        {
            Ensure.NotNull(factory, nameof(factory));

            SetCurrent(c => c.WithSchemaFactory(c.SchemaFactory.WithColumnNameFactory(factory)));
        }

        /// <summary>
        /// Gets a value indicating whether the affected row count should be verified or not.
        /// </summary>
        public bool ShouldVerifyAffectedRowCount(bool? verifyAffectedRowCount)
        {
            return verifyAffectedRowCount ?? this.VerifyAffectedRowCount;
        }

        /// <summary>
        /// Sets the current config
        /// </summary>
        internal static void SetCurrent(Func<MicroCRUDConfig, MicroCRUDConfig> updater)
        {
            lock (LockObject)
            {
                var newValue = updater(current);
                if (newValue == null)
                {
                    throw new ArgumentException("Updater returned a null object");
                }

                current = newValue;
            }
        }

        /// <summary>
        /// Creates a new <see cref="MicroCRUDConfig"/> with the specified <paramref name="dialect"/>.
        /// </summary>
        private MicroCRUDConfig WithDialect(IDialect dialect)
        {
            return new MicroCRUDConfig(dialect, this.SchemaFactory, this.VerifyAffectedRowCount);
        }

        private MicroCRUDConfig WithSchemaFactory(TableSchemaFactory factory)
        {
            return new MicroCRUDConfig(this.Dialect, factory, this.VerifyAffectedRowCount);
        }
    }
}