// <copyright file="ICustomQueryParameter.cs" company="Berkeleybross">
// Copyright (c) Berkeleybross. All rights reserved.
// </copyright>

namespace PeregrineDb.Mapping
{
    using System.Data;

    /// <summary>
    /// Implement this interface to pass an arbitrary db specific parameter to Dapper
    /// </summary>
    internal interface ICustomQueryParameter
    {
        /// <summary>
        /// Add the parameter needed to the command before it executes
        /// </summary>
        /// <param name="command">The raw command prior to execution</param>
        /// <param name="name">Parameter name</param>
        void AddParameter(IDbCommand command, string name);
    }
}
