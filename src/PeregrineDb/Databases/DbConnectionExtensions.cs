namespace PeregrineDb.Databases
{
    using System;
    using System.Data;

    internal static class DbConnectionExtensions
    {
        internal static IDbCommand MakeCommand(
            this IDbConnection connection,
            IDbTransaction transaction,
            string commandText,
            int? commandTimeout,
            CommandType? commandType,
            object parameters,
            Action<IDbCommand, object> paramReader)
        {
            var cmd = connection.CreateCommand();
            if (transaction != null)
            {
                cmd.Transaction = transaction;
            }

            cmd.CommandText = commandText;

            if (commandTimeout.HasValue)
            {
                cmd.CommandTimeout = commandTimeout.Value;
            }

            if (commandType.HasValue)
            {
                cmd.CommandType = commandType.Value;
            }

            paramReader?.Invoke(cmd, parameters);
            return cmd;
        }
    }
}