namespace PeregrineDb.Databases.Mapper
{
    /// <summary>
    /// Extends IDynamicParameters providing by-name lookup of parameter values
    /// </summary>
    internal interface IParameterLookup : IDynamicParameters
    {
        /// <summary>
        /// Get the value of the specified parameter (return null if not found)
        /// </summary>
        /// <param name="name">The name of the parameter to get.</param>
        object this[string name] { get; }
    }
}
