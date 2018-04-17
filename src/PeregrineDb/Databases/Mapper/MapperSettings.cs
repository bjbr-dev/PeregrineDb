namespace PeregrineDb.Databases.Mapper
{
    using System.Data;

    internal class MapperSettings
    {
        /// <summary>
        /// disable single result by default; prevents errors AFTER the select being detected properly
        /// </summary>
        private const CommandBehavior DefaultAllowedCommandBehaviors = ~CommandBehavior.SingleResult;

        public static MapperSettings Instance { get; set; } = new MapperSettings(DefaultAllowedCommandBehaviors);

        private readonly CommandBehavior allowedCommandBehaviors;

        public MapperSettings(CommandBehavior allowedCommandBehaviours)
        {
            this.allowedCommandBehaviors = allowedCommandBehaviours;
        }

        private MapperSettings SetAllowedCommandBehaviors(CommandBehavior behavior, bool enabled)
        {
            if (enabled)
            {
                return new MapperSettings(this.allowedCommandBehaviors | behavior);
            }
            else
            {
                return new MapperSettings(this.allowedCommandBehaviors & ~behavior);
            }
        }

        public MapperSettings UseSingleResultOptimization(bool enabled = true)
        {
            return this.SetAllowedCommandBehaviors(CommandBehavior.SingleResult, enabled);
        }

        public MapperSettings UseSingleRowOptimization(bool enabled = true)
        {
            return this.SetAllowedCommandBehaviors(CommandBehavior.SingleRow, enabled);
        }

        public CommandBehavior GetBehavior(CommandBehavior @default)
        {
            return @default & this.allowedCommandBehaviors;
        }
    }
}
