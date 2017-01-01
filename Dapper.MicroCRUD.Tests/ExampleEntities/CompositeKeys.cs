// <copyright file="CompositeKeys.cs" company="Berkeleybross">
// Copyright (c) Berkeleybross. All rights reserved.
// </copyright>
namespace Dapper.MicroCRUD.Tests.ExampleEntities
{
    using System.ComponentModel.DataAnnotations;
    using System.ComponentModel.DataAnnotations.Schema;

    public class CompositeKeys
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.None)]
        public int? Key1 { get; set; }

        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.None)]
        public int Key2 { get; set; }
    }
}