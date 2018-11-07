namespace PeregrineDb.Tests.Databases
{
    using PeregrineDb.Databases;

    internal static class CommandFactoryHelpers
    {
        public static CommandFactory CommandFactory(this IDatabase database)
        {
            return new CommandFactory(database.Config);
        }
    }
}
