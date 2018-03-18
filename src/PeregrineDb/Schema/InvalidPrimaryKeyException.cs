namespace PeregrineDb.Schema
{
    using System;

    /// <summary>
    /// Exception raised when an entity's primary key declaration is invalid for some reason.
    /// </summary>
    public class InvalidPrimaryKeyException
        : Exception
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="InvalidPrimaryKeyException"/> class.
        /// </summary>
        public InvalidPrimaryKeyException()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="InvalidPrimaryKeyException"/> class.
        /// </summary>
        public InvalidPrimaryKeyException(string message)
            : base(message)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="InvalidPrimaryKeyException"/> class.
        /// </summary>
        public InvalidPrimaryKeyException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
}