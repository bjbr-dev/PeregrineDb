namespace PeregrineDb.Databases.Mapper
{
    /// <summary>
    /// Implement this interface to change default mapping of reader columns to type members
    /// </summary>
    internal interface ITypeMap
    {
        /// <summary>
        /// Gets member mapping for column
        /// </summary>
        /// <param name="columnName">DataReader column name</param>
        /// <returns>Mapping implementation</returns>
        IMemberMap GetMember(string columnName);
    }
}
