// <copyright file="NoAutoIdentity.cs" company="Berkeleybross">
//   Copyright (c) Berkeleybross. All rights reserved.
// </copyright>
namespace Dapper.MicroCRUD.Tests.ExampleEntities
{
    using System.ComponentModel.DataAnnotations;

    public class NoAutoIdentity
    {
        [Key]
        [Required]
        public int Id { get; set; }

        public string Name { get; set; }
    }
}