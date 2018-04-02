namespace PeregrineDb.Tests.ExampleEntities
{
    using PeregrineDb.Schema;

    internal static class ExampleSchemaFactory
    {
        public static TableSchema CompositeKeys(this PeregrineConfig config)
        {
            return config.MakeSchema<CompositeKeys>();
        }

        public static TableSchema KeyAlias(this PeregrineConfig config)
        {
            return config.MakeSchema<KeyAlias>();
        }

        public static TableSchema KeyExplicit(this PeregrineConfig config)
        {
            return config.MakeSchema<KeyExplicit>();
        }

        public static TableSchema KeyNotGenerated(this PeregrineConfig config)
        {
            return config.MakeSchema<KeyNotGenerated>();
        }

        public static TableSchema PropertyAlias(this PeregrineConfig config)
        {
            return config.MakeSchema<PropertyAlias>();
        }

        public static TableSchema PropertyAllPossibleTypes(this PeregrineConfig config)
        {
            return config.MakeSchema<PropertyAllPossibleTypes>();
        }

        public static TableSchema PropertyComputed(this PeregrineConfig config)
        {
            return config.MakeSchema<PropertyComputed>();
        }

        public static TableSchema PropertyGenerated(this PeregrineConfig config)
        {
            return config.MakeSchema<PropertyGenerated>();
        }

        public static TableSchema Dog(this PeregrineConfig config)
        {
            return config.MakeSchema<Dog>();
        }

        public static TableSchema NoColumns(this PeregrineConfig config)
        {
            return config.MakeSchema<NoColumns>();
        }

        public static TableSchema TempAllPossibleTypes(this PeregrineConfig config)
        {
            return config.MakeSchema<TempAllPossibleTypes>();
        }

        public static TableSchema MakeSchema<TEntity>(this PeregrineConfig config)
        {
            return config.GetTableSchema(typeof(TEntity));
        }
    }
}