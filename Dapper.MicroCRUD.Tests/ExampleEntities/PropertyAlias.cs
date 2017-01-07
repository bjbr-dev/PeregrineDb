// <copyright file="PropertyAlias.cs" company="Berkeleybross">
// Copyright (c) Berkeleybross. All rights reserved.
// </copyright>
namespace Dapper.MicroCRUD.Tests.ExampleEntities
{
    using System.ComponentModel.DataAnnotations.Schema;

    [Table(nameof(PropertyAlias))]
    public class PropertyAlias
    {
        public int Id { get; set; }

        [Column("YearsOld")]
        public int Age { get; set; }
    }
}
