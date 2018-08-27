// <copyright file="IDynamicParameters.cs" company="Berkeleybross">
// Copyright (c) Berkeleybross. All rights reserved.
// </copyright>

namespace PeregrineDb.Mapping
{
    using System.Data;
    using PeregrineDb.Databases.Mapper;

    /// <summary>
    /// Implement this interface to pass an arbitrary db specific set of parameters to Dapper
    /// </summary>
    internal interface IDynamicParameters
    {
        /// <summary>
        /// Add all the parameters needed to the command just before it executes
        /// </summary>
        /// <param name="command">The raw command prior to execution</param>
        /// <param name="identity">Information about the query</param>
        void AddParameters(IDbCommand command, Identity identity);
    }
}