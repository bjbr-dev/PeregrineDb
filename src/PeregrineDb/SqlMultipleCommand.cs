// <copyright file="SqlMultipleCommand.cs" company="Berkeleybross">
// Copyright (c) Berkeleybross. All rights reserved.
// </copyright>
namespace PeregrineDb
{
    using System.Collections.Generic;

    public struct SqlMultipleCommand<T>
    {
        public SqlMultipleCommand(string commandText, IEnumerable<T> parameters)
        {
            this.CommandText = commandText;
            this.Parameters = parameters;
        }

        public string CommandText { get; }

        public IEnumerable<T> Parameters { get; }

        public void Deconstruct(out string sql, out IEnumerable<T> parameters)
        {
            sql = this.CommandText;
            parameters = this.Parameters;
        }
    }
}