// <copyright file="NonPluralizingTableNameFactory.cs" company="Berkeleybross">
// Copyright (c) Berkeleybross. All rights reserved.
// </copyright>
namespace Dapper.MicroCRUD.Schema
{
    using System;
    using System.ComponentModel.DataAnnotations.Schema;

    /// <summary>
    /// Implements the <see cref="ITableNameFactory"/> by using the <see cref="TableAttribute"/> if present,
    /// otherwise just takes the class name.
    /// </summary>
    public class NonPluralizingTableNameFactory
        : DefaultTableNameFactory
    {
        /// <inheritdoc />
        protected override string GetTableNameFromType(Type type)
        {
            return type.Name;
        }
    }
}