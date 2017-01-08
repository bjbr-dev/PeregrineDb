// <copyright file="PropertyEnum.cs" company="Berkeleybross">
// Copyright (c) Berkeleybross. All rights reserved.
// </copyright>
namespace Dapper.MicroCRUD.Tests.ExampleEntities
{
    using System.ComponentModel.DataAnnotations.Schema;

    [Table(nameof(PropertyEnum))]
    public class PropertyEnum
    {
        public int Id { get; set; }

        public Color? FavoriteColor { get; set; }
    }
}