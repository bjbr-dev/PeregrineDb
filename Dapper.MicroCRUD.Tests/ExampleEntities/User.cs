// <copyright file="User.cs" company="Berkeleybross">
// Copyright (c) Berkeleybross. All rights reserved.
// </copyright>
namespace Dapper.MicroCRUD.Tests.ExampleEntities
{
    using System.ComponentModel.DataAnnotations.Schema;

    [Table("Users")]
    public class User
    {
        public int Id { get; set; }

        public string Name { get; set; }

        public int Age { get; set; }
    }
}