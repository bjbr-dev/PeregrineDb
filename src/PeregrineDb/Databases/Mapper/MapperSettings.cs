namespace PeregrineDb.Databases.Mapper
{
    using System;
    using System.Data;

    /// <summary>
    /// Permits specifying certain SqlMapper values globally.
    /// </summary>
    internal static class MapperSettings
    {
        // disable single result by default; prevents errors AFTER the select being detected properly
        private const CommandBehavior DefaultAllowedCommandBehaviors = ~CommandBehavior.SingleResult;
        internal static CommandBehavior AllowedCommandBehaviors { get; private set; } = DefaultAllowedCommandBehaviors;
        private static void SetAllowedCommandBehaviors(CommandBehavior behavior, bool enabled)
        {
            if (enabled) AllowedCommandBehaviors |= behavior;
            else AllowedCommandBehaviors &= ~behavior;
        }
        /// <summary>
        /// Gets or sets whether Dapper should use the CommandBehavior.SingleResult optimization
        /// </summary>
        /// <remarks>Note that a consequence of enabling this option is that errors that happen <b>after</b> the first select may not be reported</remarks>
        public static bool UseSingleResultOptimization
        {
            get { return (AllowedCommandBehaviors & CommandBehavior.SingleResult) != 0; }
            set { SetAllowedCommandBehaviors(CommandBehavior.SingleResult, value); }
        }
        /// <summary>
        /// Gets or sets whether Dapper should use the CommandBehavior.SingleRow optimization
        /// </summary>
        /// <remarks>Note that on some DB providers this optimization can have adverse performance impact</remarks>
        public static bool UseSingleRowOptimization
        {
            get { return (AllowedCommandBehaviors & CommandBehavior.SingleRow) != 0; }
            set { SetAllowedCommandBehaviors(CommandBehavior.SingleRow, value); }
        }

        internal static bool DisableCommandBehaviorOptimizations(CommandBehavior behavior, Exception ex)
        {
            if (AllowedCommandBehaviors == DefaultAllowedCommandBehaviors
                && (behavior & (CommandBehavior.SingleResult | CommandBehavior.SingleRow)) != 0)
            {
                if (ex.Message.Contains(nameof(CommandBehavior.SingleResult))
                    || ex.Message.Contains(nameof(CommandBehavior.SingleRow)))
                { // some providers just just allow these, so: try again without them and stop issuing them
                    SetAllowedCommandBehaviors(CommandBehavior.SingleResult | CommandBehavior.SingleRow, false);
                    return true;
                }
            }
            return false;
        }

        public static CommandBehavior GetBehavior(CommandBehavior @default)
        {
            return @default & MapperSettings.AllowedCommandBehaviors;
        }
    }
}
