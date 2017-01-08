// <copyright file="AffectedRowCountException.cs" company="Berkeleybross">
// Copyright (c) Berkeleybross. All rights reserved.
// </copyright>
namespace Dapper.MicroCRUD.SqlCommands
{
    using System;
    using System.Runtime.Serialization;

    /// <summary>
    /// Exception thrown when the number of affected rows in a query does not match the expected number.
    /// </summary>
    public class AffectedRowCountException
        : Exception
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="AffectedRowCountException"/> class.
        /// </summary>
        public AffectedRowCountException()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="AffectedRowCountException"/> class.
        /// </summary>
        public AffectedRowCountException(string message)
            : base(message)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="AffectedRowCountException"/> class.
        /// </summary>
        public AffectedRowCountException(string message, Exception innerException)
            : base(message, innerException)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="AffectedRowCountException"/> class.
        /// </summary>
        protected AffectedRowCountException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
        }
    }
}