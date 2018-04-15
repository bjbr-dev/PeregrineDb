namespace PeregrineDb.Databases.Mapper
{
    /// <summary>
    /// Extends IDynamicParameters with facilities for executing callbacks after commands have completed
    /// </summary>
    internal interface IParameterCallbacks : IDynamicParameters
    {
        /// <summary>
        /// Invoked when the command has executed
        /// </summary>
        void OnCompleted();
    }
}
