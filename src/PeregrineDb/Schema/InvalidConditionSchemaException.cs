namespace PeregrineDb.Schema
{
    using System;

    public class InvalidConditionSchemaException
        : Exception
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="InvalidConditionSchemaException"/> class.
        /// </summary>
        public InvalidConditionSchemaException()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="InvalidConditionSchemaException"/> class.
        /// </summary>
        public InvalidConditionSchemaException(string message)
            : base(message)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="InvalidConditionSchemaException"/> class.
        /// </summary>
        public InvalidConditionSchemaException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
}