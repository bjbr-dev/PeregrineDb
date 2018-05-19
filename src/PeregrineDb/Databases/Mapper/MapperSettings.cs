namespace PeregrineDb.Databases.Mapper
{
    using System.Data;

    internal class MapperSettings
    {
        /// <summary>
        /// disable single result by default; prevents errors AFTER the select being detected properly
        /// </summary>
        public static MapperSettings Instance { get; set; } = new MapperSettings(~CommandBehavior.SingleResult);

        private readonly CommandBehavior allowedCommandBehaviors;

        public MapperSettings(CommandBehavior allowedCommandBehaviours)
        {
            this.allowedCommandBehaviors = allowedCommandBehaviours;
        }

        public CommandBehavior GetBehavior(CommandBehavior behavior)
        {
            return behavior & this.allowedCommandBehaviors;
        }
    }
}