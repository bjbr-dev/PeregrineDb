// <copyright file="KeyAlias.cs" company="Berkeleybross">
// Copyright (c) Berkeleybross. All rights reserved.
// </copyright>

namespace Dapper.MicroCRUD.Tests.ExampleEntities
{
    using System.ComponentModel.DataAnnotations;

    public class KeyAlias
    {
        [Key]
        public int Key { get; set; }

        public string Name { get; set; }
    }
}