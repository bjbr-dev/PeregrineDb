// <copyright file="PostgresNameEscaper.cs" company="Berkeleybross">
// Copyright (c) Berkeleybross. All rights reserved.
// </copyright>

namespace PeregrineDb.Dialects.Postgres
{
    using PeregrineDb.Schema;

    public class PostgresNameEscaper
        : ISqlNameEscaper
    {
        public string EscapeColumnName(string name)
        {
            return name;
        }

        public string EscapeTableName(string tableName)
        {
            return tableName;
        }

        public string EscapeTableName(string schema, string tableName)
        {
            return schema + "." + tableName;
        }
    }
}