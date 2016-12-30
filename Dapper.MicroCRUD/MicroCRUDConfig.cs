// <copyright file="MicroCRUDConfig.cs" company="Berkeleybross">
// Copyright (c) Berkeleybross. All rights reserved.
// </copyright>
namespace Dapper.MicroCRUD
{
    using Dapper.MicroCRUD.Entities;

    /// <summary>
    /// Defines the configuration of MicroCRUD.
    /// </summary>
    public static class MicroCRUDConfig
    {
        /// <summary>
        /// Gets or sets the default dialect
        /// </summary>
        public static Dialect DefaultDialect { get; set; } = Dialect.SqlServer2012;

        /// <summary>
        /// Gets or sets the resolver to use when getting the table name of an entity.
        /// </summary>
        public static ITableNameResolver TableNameResolver { get; set; } =
            new DefaultTableNameResolver();

        /// <summary>
        /// Gets or sets the resolver to use when getting the column name of an entity field.
        /// </summary>
        public static IColumnNameResolver ColumnNameResolver { get; set; } =
            new DefaultColumnNameResolver();
    }
}