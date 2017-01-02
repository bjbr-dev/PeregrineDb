// <copyright file="PropertyNotMapped.cs" company="Berkeleybross">
// Copyright (c) Berkeleybross. All rights reserved.
// </copyright>
namespace Dapper.MicroCRUD.Tests.ExampleEntities
{
    using System.ComponentModel.DataAnnotations.Schema;

    public class PropertyNotMapped
    {
        public int Id { get; set; }

        public string Firstname { get; set; }

        public string LastName { get; set; }

        [NotMapped]
        public int Age { get; set; }

        [NotMapped]
        public string FullName => this.Firstname + " " + this.LastName;
    }
}