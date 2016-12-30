// <copyright file="CompositeKeys.cs" company="Berkeleybross">
// Copyright (c) Berkeleybross. All rights reserved.
// </copyright>
namespace Dapper.MicroCRUD.Tests.ExampleEntities
{
    using System.ComponentModel.DataAnnotations;

    public class CompositeKeys
    {
        [Key]
        public int Key1 { get; set; }

        [Key]
        public int Key2 { get; set; }
    }
}