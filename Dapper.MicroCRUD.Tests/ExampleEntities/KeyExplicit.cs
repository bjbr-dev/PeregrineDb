// <copyright file="KeyExplicit.cs" company="Berkeleybross">
// Copyright (c) Berkeleybross. All rights reserved.
// </copyright>
namespace Dapper.MicroCRUD.Tests.ExampleEntities
{
    using System.ComponentModel.DataAnnotations;
    using System.ComponentModel.DataAnnotations.Schema;

    [Table(nameof(KeyExplicit))]
    public class KeyExplicit
    {
        [Key]
        public int Key { get; set; }

        public string Name { get; set; }
    }
}