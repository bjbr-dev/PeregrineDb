// <copyright file="NoKey.cs" company="Berkeleybross">
// Copyright (c) Berkeleybross. All rights reserved.
// </copyright>
namespace Dapper.MicroCRUD.Tests.ExampleEntities
{
    using System.ComponentModel.DataAnnotations.Schema;

    [Table(nameof(NoKey))]
    public class NoKey
    {
        public string Name { get; set; }

        public int Age { get; set; }
    }
}