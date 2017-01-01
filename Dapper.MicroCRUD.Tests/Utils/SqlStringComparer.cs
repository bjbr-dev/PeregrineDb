// <copyright file="SqlStringComparer.cs" company="Berkeleybross">
// Copyright (c) Berkeleybross. All rights reserved.
// </copyright>
namespace Dapper.MicroCRUD.Tests.Utils
{
    using System;
    using System.Collections.Generic;

    public class SqlStringComparer
        : IComparer<string>
    {
        private readonly IComparer<string> comparer;

        public SqlStringComparer(IComparer<string> comparer)
        {
            this.comparer = comparer;
        }

        public static SqlStringComparer Instance { get; } = new SqlStringComparer(StringComparer.Ordinal);

        public int Compare(string x, string y)
        {
            x = x?.Replace("\r\n", "\n");
            y = y?.Replace("\r\n", "\n");

            return this.comparer.Compare(x, y);
        }
    }
}