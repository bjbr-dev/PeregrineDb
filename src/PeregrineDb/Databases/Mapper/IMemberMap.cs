namespace PeregrineDb.Databases.Mapper
{
    using System;
    using System.Reflection;
    
    /// <summary>
    /// Implements this interface to provide custom member mapping
    /// </summary>
    internal interface IMemberMap
    {
        /// <summary>
        /// Source DataReader column name
        /// </summary>
        string ColumnName { get; }

        /// <summary>
        ///  Target member type
        /// </summary>
        Type MemberType { get; }

        /// <summary>
        /// Target property
        /// </summary>
        PropertyInfo Property { get; }

        /// <summary>
        /// Target field
        /// </summary>
        FieldInfo Field { get; }

        /// <summary>
        /// Target constructor parameter
        /// </summary>
        ParameterInfo Parameter { get; }
    }
}
