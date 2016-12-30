// <copyright file="KeyString.cs" company="Berkeleybross">
// Copyright (c) Berkeleybross. All rights reserved.
// </copyright>
namespace Dapper.MicroCRUD.Tests.ExampleEntities
{
    using System.ComponentModel.DataAnnotations;

    public class KeyString
    {
        [Key]
        [Required]
        public string Name { get; set; }

        public int Age { get; set; }
    }
}