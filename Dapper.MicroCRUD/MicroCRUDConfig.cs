// <copyright file="MicroCRUDConfig.cs" company="Berkeleybross">
// Copyright (c) Berkeleybross. All rights reserved.
// </copyright>
namespace Dapper.MicroCRUD
{
    using Dapper.MicroCRUD.Schema;
    using Dapper.MicroCRUD.Utils;

    /// <summary>
    /// Defines the configuration for MicroCRUD.
    /// </summary>
    public static class MicroCRUDConfig
    {
        private static volatile Dialect defaultDialect = Dialect.SqlServer2012;

        /// <summary>
        /// Gets or sets the default dialect
        /// </summary>
        public static Dialect DefaultDialect
        {
            get { return defaultDialect; }
            set { defaultDialect = value; }
        }

        /// <summary>
        /// Sets the method used to get the column name from a property.
        /// </summary>
        public static void SetTableNameFactory(ITableNameFactory factory)
        {
            Ensure.NotNull(factory, nameof(factory));

            TableSchemaFactory.SetCurrent(f => f.WithTableNameFactory(factory));
        }

        /// <summary>
        /// Sets the method used to get the column name from a property.
        /// </summary>
        public static void SetColumnNameFactory(IColumnNameFactory factory)
        {
            Ensure.NotNull(factory, nameof(factory));

            TableSchemaFactory.SetCurrent(f => f.WithColumnNameFactory(factory));
        }
    }
}