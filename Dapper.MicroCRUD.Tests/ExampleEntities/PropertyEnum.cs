// <copyright file="PropertyEnum.cs" company="Berkeleybross">
// Copyright (c) Berkeleybross. All rights reserved.
// </copyright>
namespace Dapper.MicroCRUD.Tests.ExampleEntities
{
    using System.ComponentModel.DataAnnotations.Schema;

    [Table(nameof(PropertyEnum))]
    public class PropertyEnum
    {
        public enum Color
        {
            /// <summary>
            /// The color Red
            /// </summary>
            Red,

            /// <summary>
            /// The color Green
            /// </summary>
            Green,

            /// <summary>
            /// The color Blue
            /// </summary>
            Blue
        }

        public int Id { get; set; }

        public Color? FavoriteColor { get; set; }
    }
}