// <copyright file="MapperSettings.cs" company="Berkeleybross">
// Copyright (c) Berkeleybross. All rights reserved.
// </copyright>

namespace PeregrineDb.Databases.Mapper
{
    using System.Data;

    internal class MapperSettings
    {
        private readonly CommandBehavior allowedCommandBehaviors;

        public MapperSettings(CommandBehavior allowedCommandBehaviours)
        {
            this.allowedCommandBehaviors = allowedCommandBehaviours;
        }

        /// <summary>
        /// disable single result by default; prevents errors AFTER the select being detected properly
        /// </summary>
        public static MapperSettings Instance { get; set; } = new MapperSettings(~CommandBehavior.SingleResult);

        public CommandBehavior GetBehavior(CommandBehavior behavior)
        {
            return behavior & this.allowedCommandBehaviors;
        }
    }
}