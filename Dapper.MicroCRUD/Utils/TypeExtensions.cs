// <copyright file="TypeExtensions.cs" company="Berkeleybross">
// Copyright (c) Berkeleybross. All rights reserved.
// </copyright>
namespace Dapper.MicroCRUD.Utils
{
    using System;

    /// <summary>
    /// Extension methods for the <see cref="Type"/> class.
    /// </summary>
    internal static class TypeExtensions
    {
        /// <summary>
        /// If the type is nullable, gets the underlying type of the nullable, otherwise returns the type.
        /// </summary>
        public static Type GetUnderlyingType(this Type type)
        {
            var underlyingType = Nullable.GetUnderlyingType(type);
            return underlyingType ?? type;
        }
    }
}