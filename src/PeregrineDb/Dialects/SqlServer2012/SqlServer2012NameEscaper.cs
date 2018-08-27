// <copyright file="SqlServer2012NameEscaper.cs" company="Berkeleybross">
// Copyright (c) Berkeleybross. All rights reserved.
// </copyright>

namespace PeregrineDb.Dialects.SqlServer2012
{
    using PeregrineDb.Schema;

    public class SqlServer2012NameEscaper
        : ISqlNameEscaper
    {
        /// <inheritdoc />
        public string EscapeColumnName(string name)
        {
            return "[" + name + "]";
        }

        /// <inheritdoc />
        public string EscapeTableName(string tableName)
        {
            return "[" + tableName + "]";
        }

        /// <inheritdoc />
        public string EscapeTableName(string schema, string tableName)
        {
            return "[" + schema + "].[" + tableName + "]";
        }
    }
}