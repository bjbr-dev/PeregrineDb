// <copyright file="DeserializerState.cs" company="Berkeleybross">
// Copyright (c) Berkeleybross. All rights reserved.
// </copyright>

namespace PeregrineDb.Databases.Mapper
{
    using System;
    using System.Data;

    internal struct DeserializerState
    {
        public readonly int Hash;

        public readonly Func<IDataReader, object> Func;

        public DeserializerState(int hash, Func<IDataReader, object> func)
        {
            this.Hash = hash;
            this.Func = func;
        }
    }
}
