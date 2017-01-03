// <copyright file="MicroCRUDConfig.cs" company="Berkeleybross">
// Copyright (c) Berkeleybross. All rights reserved.
// </copyright>
namespace Dapper.MicroCRUD
{
    /// <summary>
    /// Defines the configuration for MicroCRUD.
    /// </summary>
    public static class MicroCRUDConfig
    {
        /// <summary>
        /// Gets or sets the default dialect
        /// </summary>
        public static Dialect DefaultDialect { get; set; } = Dialect.SqlServer2012;
    }
}