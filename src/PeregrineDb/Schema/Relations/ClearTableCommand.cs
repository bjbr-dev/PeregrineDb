// <copyright file="ClearTableCommand.cs" company="Berkeleybross">
// Copyright (c) Berkeleybross. All rights reserved.
// </copyright>

namespace PeregrineDb.Schema.Relations
{
    internal class ClearTableCommand
    {
        public ClearTableCommand(string tableName)
        {
            this.TableName = tableName;
        }

        public string TableName { get;  }
    }
}