// <copyright file="PropertyAllPossibleTypes.cs" company="Berkeleybross">
// Copyright (c) Berkeleybross. All rights reserved.
// </copyright>
namespace Dapper.MicroCRUD.Tests.ExampleEntities
{
    using System;

    /// <remarks>
    /// Except enum, which is a different class :/
    /// </remarks>
    public class PropertyAllPossibleTypes
    {
        public int Id { get; set; }

        public short Int16Property { get; set; }

        public short? NullableInt16Property { get; set; }

        public int Int32Property { get; set; }

        public int? NullableInt32Property { get; set; }

        public long Int64Property { get; set; }

        public long? NullableInt64Property { get; set; }

        public float SingleProperty { get; set; }

        public float? NullableSingleProperty { get; set; }

        public double DoubleProperty { get; set; }

        public double? NullableDoubleProperty { get; set; }

        public decimal DecimalProperty { get; set; }

        public decimal? NullableDecimalProperty { get; set; }

        public bool BoolProperty { get; set; }

        public bool? NullableBoolProperty { get; set; }

        public string StringProperty { get; set; }

        public char CharProperty { get; set; }

        public char? NullableCharProperty { get; set; }

        public Guid GuidProperty { get; set; }

        public Guid? NullableGuidProperty { get; set; }

        public DateTime DateTimeProperty { get; set; }

        public DateTime? NullableDateTimeProperty { get; set; }

        public DateTimeOffset DateTimeOffsetProperty { get; set; }

        public DateTimeOffset? NullableDateTimeOffsetProperty { get; set; }

        public byte[] ByteArrayProperty { get; set; }
    }
}