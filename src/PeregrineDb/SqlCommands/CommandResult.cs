namespace PeregrineDb.SqlCommands
{
    /// <summary>
    /// Represents the result of a SQL command.
    /// </summary>
    public struct CommandResult
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="CommandResult"/> struct.
        /// </summary>
        public CommandResult(int numRowsAffected)
        {
            this.NumRowsAffected = numRowsAffected;
        }

        /// <summary>
        /// Gets the number of rows affected
        /// </summary>
        public int NumRowsAffected { get; }

        /// <summary>
        /// Throws an exception if the <see cref="NumRowsAffected"/> does not match the <paramref name="expectedCount"/>
        /// </summary>
        public void ExpectingAffectedRowCountToBe(int expectedCount)
        {
            if (this.NumRowsAffected != expectedCount)
            {
                throw new AffectedRowCountException(
                    $"Expected {expectedCount} rows to be updated, but was actually {this.NumRowsAffected}");
            }
        }
    }
}